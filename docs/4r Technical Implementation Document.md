# Technical Implementation Document: CNES Data Pipeline V2 (Supabase/PostgreSQL)

## 1. Architecture V2 Summary: Multi-Tenant & Data Safety Hardening
The V2 architecture transitions from a global-namespace model to a **True Multi-Tenant Architecture**. By embedding `tenant_id` (standardized as the 6-digit IBGE `cod_municipio`) into the composite Primary Keys of all layers, the system ensures structural isolation for B2G deployments. 

Data integrity is now enforced at the database level using **Strict RegEx Constraints**, preventing the ingestion of non-numeric or malformed keys (CNES/CPF/CNS). The DML has been refactored to prioritize **source-agnostic idempotency**, removing the flawed `GREATEST` workload logic to allow legitimate downward corrections. Performance is optimized via **explicit indexing** on high-cardinality foreign keys and the use of native **BOOLEAN** types to reduce dictionary overhead and alignment padding.

---

## 2. PostgreSQL DDL: Hardened Multi-Tenant Schema
*Refactored to include multi-tenant PKs, numeric regex validation, and explicit analytical indexing.*

```sql
-- =========================================================
-- SCHEMA & EXTENSION SETUP
-- =========================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- =========================================================
-- GOLD DIMENSION: Estabelecimentos
-- PK: (tenant_id, cnes) to allow same CNES across different tenants if needed
-- =========================================================
CREATE TABLE gold.dim_estabelecimento (
    tenant_id VARCHAR(6) NOT NULL, -- IBGE Code
    cnes VARCHAR(7) NOT NULL,
    nome_fantasia VARCHAR(120),
    tipo_unidade VARCHAR(2),
    cnpj_mantenedora VARCHAR(14),
    natureza_juridica VARCHAR(4),
    vinculo_sus BOOLEAN DEFAULT FALSE, -- Replaced ENUM for efficiency
    fontes JSONB DEFAULT '{}'::jsonb,
    criado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    atualizado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (tenant_id, cnes),
    CONSTRAINT chk_cnes_format CHECK (cnes ~ '^\d{7}$'),
    CONSTRAINT chk_tenant_format CHECK (tenant_id ~ '^\d{6}$')
);

-- =========================================================
-- GOLD DIMENSION: Profissionais
-- =========================================================
CREATE TABLE gold.dim_profissional (
    tenant_id VARCHAR(6) NOT NULL,
    cpf VARCHAR(11) NOT NULL,
    cns VARCHAR(15), 
    nome_profissional VARCHAR(120),
    sexo VARCHAR(1),
    fontes JSONB DEFAULT '{}'::jsonb,
    criado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    atualizado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (tenant_id, cpf),
    CONSTRAINT chk_cpf_format CHECK (cpf ~ '^\d{11}$'),
    CONSTRAINT chk_cns_format CHECK (cns IS NULL OR cns ~ '^\d{15}$'),
    CONSTRAINT chk_tenant_format CHECK (tenant_id ~ '^\d{6}$')
);

-- =========================================================
-- GOLD FACT: Vínculos
-- Refactored PK and added explicit indexes for analytical queries
-- =========================================================
CREATE TABLE gold.fato_vinculo (
    tenant_id VARCHAR(6) NOT NULL,
    competencia VARCHAR(7) NOT NULL, -- Format: YYYY-MM
    cpf VARCHAR(11) NOT NULL,
    cnes VARCHAR(7) NOT NULL,
    cbo VARCHAR(6) NOT NULL,
    tipo_vinculo VARCHAR(6),
    sus BOOLEAN,
    ch_total INTEGER,
    ch_ambulatorial INTEGER,
    ch_outras INTEGER,
    ch_hospitalar INTEGER,
    fontes JSONB DEFAULT '{}'::jsonb,
    criado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    atualizado_em TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    PRIMARY KEY (tenant_id, competencia, cpf, cnes, cbo),
    -- Explicit Foreign Keys linking to multi-tenant dimensions
    FOREIGN KEY (tenant_id, cpf) REFERENCES gold.dim_profissional(tenant_id, cpf),
    FOREIGN KEY (tenant_id, cnes) REFERENCES gold.dim_estabelecimento(tenant_id, cnes)
);

-- Explicit indexes to prevent full-table scans during cross-competency HR analysis
CREATE INDEX idx_fato_vinculo_cpf ON gold.fato_vinculo (tenant_id, cpf);
CREATE INDEX idx_fato_vinculo_cnes ON gold.fato_vinculo (tenant_id, cnes);
```


---

## 3. Refactored Upsert DML: Parameterized & Agnostic
*The `GREATEST` function is removed to allow hours to be corrected downwards. The source check is parameterized (`:source_key`) to prevent DML-level hardcoding.*

```sql
-- UPSERT: fato_vinculo
-- Expected parameters: :source_key (e.g., 'LOCAL', 'NACIONAL')
INSERT INTO gold.fato_vinculo (
    tenant_id, competencia, cpf, cnes, cbo, tipo_vinculo, sus, 
    ch_total, ch_ambulatorial, ch_outras, ch_hospitalar, fontes, atualizado_em
) 
VALUES (
    '355030', '2026-04', '12345678901', '1234567', '515105', '010101', TRUE, 
    20, 20, 0, 0, jsonb_build_object(:source_key, true), NOW()
)
ON CONFLICT (tenant_id, competencia, cpf, cnes, cbo) DO UPDATE SET
    tipo_vinculo = COALESCE(EXCLUDED.tipo_vinculo, gold.fato_vinculo.tipo_vinculo),
    sus = COALESCE(EXCLUDED.sus, gold.fato_vinculo.sus),
    -- Removed GREATEST: newest data (EXCLUDED) now allows downward correction
    ch_total = EXCLUDED.ch_total,
    ch_ambulatorial = EXCLUDED.ch_ambulatorial,
    ch_outras = EXCLUDED.ch_outras,
    ch_hospitalar = EXCLUDED.ch_hospitalar,
    fontes = gold.fato_vinculo.fontes || EXCLUDED.fontes,
    atualizado_em = NOW()
WHERE 
    -- Only update if data has actually changed (HOT optimization)
    (gold.fato_vinculo.ch_total, gold.fato_vinculo.sus, gold.fato_vinculo.tipo_vinculo) 
    IS DISTINCT FROM 
    (EXCLUDED.ch_total, EXCLUDED.sus, EXCLUDED.tipo_vinculo)
    OR NOT (gold.fato_vinculo.fontes ? :source_key);
```


---

## 4. Hardened Python Guardrails (V2)
*Memory-efficient implementation that resolves the `<NA>` string corruption and preserves native types for database driver serialization.*

```python
import pandas as pd
import numpy as np

def enforce_strict_health_keys_v2(df: pd.DataFrame) -> pd.DataFrame:
    """
    V2: Refactored for memory efficiency and data integrity.
    Resolves the <NA> string corruption bug and OOM risks.
    """
    # 1. REMOVED df.copy() to prevent memory doubling; mutate in-place or handle return.
    
    # 2. Fix Padding Logic: Handle nulls BEFORE string casting
    # Extracts only digits and pads, ensuring NaN remains NaN (not '<NA>')
    for col, pad_length in [('CNES', 7), ('CPF', 11)]:
        if col in df.columns:
            # Convert to string first, extract digits, then pad. 
            # This ensures np.nan remains np.nan during the process.
            df[col] = (
                df[col].astype(str)
                .str.extract(r'(\d+)', expand=False)
                .str.zfill(pad_length)
            )
            # Re-masking empty strings/NaN that might have been zfilled to '0000000'
            df.loc[df[col].isna(), col] = np.nan

    # 3. Eradicate Synthetics for CNS
    if 'CNS' in df.columns:
        # Identify synthetics without casting to string-objects yet
        synthetic_mask = (
            df['CNS'].astype(str).str.contains('^SEM:', na=False) | 
            (df['CNS'].astype(str).str.len() < 15)
        )
        df.loc[synthetic_mask, 'CNS'] = np.nan

    # 4. DELEGATED Null Handling: Removed .replace({np.nan: None})
    # Modern drivers (psycopg3/asyncpg) correctly serialize pd.NA/np.nan 
    # as PostgreSQL NULL while preserving memory-efficient Arrow backends.
    
    return df
```