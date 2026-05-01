# Aprovação Manual de Solicitações de Acesso (v1.1)

## Visão Geral

Em v1.1 o fluxo de signup é **JIT pendente**: usuário autentica via OIDC,
SPA detecta `tenant_ids` vazio e mostra `/access-pending`. Lá o usuário
escolhe um município e descreve o motivo. Backend grava em
`dashboard.access_requests` com `status='pending'`.

Aprovação/rejeição é manual via SQL nesta v1.1 (UI administrativa fica
para v1.2). Este runbook documenta os comandos canônicos.

```
[user signup form]  →  access_requests (pending)  →  [DBA roda SQL deste doc]  →  user_tenants + audit_log
```

## Pré-requisitos

- Acesso `psql` ao Postgres central com role `cnesdata` (rolsuper) ou
  equivalente que possa setar `app.tenant_id`
- ID UUID do admin que está aprovando (selecionar antes — usado em
  `reviewed_by` e em `audit_log.user_id`)
- Tenant alvo deve existir em `gold.dim_municipio` (validação opcional
  abaixo)

```bash
psql "$DB_URL"
```

---

## Listar pending

```sql
SELECT
    ar.id,
    u.email,
    u.display_name,
    ar.tenant_id,
    m.nome AS municipio_nome,
    m.uf,
    ar.motivation,
    ar.requested_at
FROM dashboard.access_requests ar
JOIN dashboard.users u ON u.id = ar.user_id
LEFT JOIN gold.dim_municipio m ON m.ibge6 = ar.tenant_id
WHERE ar.status = 'pending'
ORDER BY ar.requested_at;
```

Anotar:
- `ar.id` (UUID da solicitação)
- `u.id` será obtido implícito pelo JOIN — pode acrescentar `u.id` ao SELECT
  se preferir copiar manualmente
- `ar.tenant_id` (CHAR(6) — deve casar com `gold.dim_municipio.ibge6`)

## Resolver UUID do admin

```sql
SELECT id, email, role
FROM dashboard.users
WHERE email = 'admin@cnesdata.gov.br' AND revoked_at IS NULL;
```

Guardar como `:admin_id` (psql variable):

```sql
\set admin_id '''<uuid-aqui>'''
```

---

## Aprovar (com transação)

`user_tenants` tem RLS **FORCE**, exigindo `app.tenant_id` setado para o
tenant que será inserido. Tudo dentro de transação para atomicidade
(`access_requests` update + `user_tenants` insert + `audit_log`).

```sql
BEGIN;

-- 1. Setar contexto do tenant alvo (substituir 354130 pelo tenant da request)
SET LOCAL app.tenant_id = '354130';

-- 2. Inserir vínculo (idempotente via ON CONFLICT)
INSERT INTO dashboard.user_tenants (user_id, tenant_id)
SELECT user_id, tenant_id
FROM dashboard.access_requests
WHERE id = '<request-id-aqui>'
  AND status = 'pending'
ON CONFLICT (user_id, tenant_id) DO NOTHING;

-- 3. Marcar solicitação como aprovada
UPDATE dashboard.access_requests
SET status       = 'approved',
    reviewed_at  = NOW(),
    reviewed_by  = :admin_id,
    review_notes = 'Aprovado — gestor verificado em <ofício/canal>.'
WHERE id = '<request-id-aqui>'
  AND status = 'pending';

-- 4. Audit (tenant_id NULL é permitido pela policy audit_log_isolation
--    — ação administrativa cross-tenant)
INSERT INTO dashboard.audit_log (user_id, tenant_id, action, metadata)
VALUES (
    :admin_id,
    NULL,
    'approve_access',
    jsonb_build_object(
        'request_id', '<request-id-aqui>',
        'tenant_id', '354130'
    )
);

COMMIT;
```

Validar pós-commit:

```sql
SET LOCAL app.tenant_id = '354130';
SELECT u.email, ut.tenant_id, ar.status, ar.reviewed_at
FROM dashboard.access_requests ar
JOIN dashboard.users u ON u.id = ar.user_id
JOIN dashboard.user_tenants ut ON ut.user_id = ar.user_id AND ut.tenant_id = ar.tenant_id
WHERE ar.id = '<request-id-aqui>';
```

Esperado: 1 linha, `status='approved'`, `reviewed_at` recente.

---

## Rejeitar

Sem `user_tenants` insert; apenas marca status e audit:

```sql
BEGIN;

UPDATE dashboard.access_requests
SET status       = 'rejected',
    reviewed_at  = NOW(),
    reviewed_by  = :admin_id,
    review_notes = '<motivo da rejeição — ex.: e-mail não corresponde a órgão público>'
WHERE id = '<request-id-aqui>'
  AND status = 'pending';

INSERT INTO dashboard.audit_log (user_id, tenant_id, action, metadata)
VALUES (
    :admin_id,
    NULL,
    'reject_access',
    jsonb_build_object(
        'request_id', '<request-id-aqui>',
        'tenant_id', '<tenant-id-da-request>'
    )
);

COMMIT;
```

---

## Reativar usuário aprovado posteriormente

Se uma solicitação foi rejeitada e depois decide-se aprovar (ou usuário
abriu nova solicitação para o mesmo tenant após rejeição):

1. UNIQUE `(user_id, tenant_id)` em `access_requests` exige **deletar a
   row antiga rejeitada** antes do usuário re-submeter via SPA, OU
2. Aprovar diretamente via SQL inserindo em `user_tenants` mesmo sem nova
   request — registrar em `audit_log` com metadata explícita.

Preferir (2) quando o canal de aprovação foi externo (e-mail, ofício):

```sql
BEGIN;
SET LOCAL app.tenant_id = '<tenant>';

INSERT INTO dashboard.user_tenants (user_id, tenant_id)
VALUES ('<user-uuid>', '<tenant>')
ON CONFLICT DO NOTHING;

INSERT INTO dashboard.audit_log (user_id, tenant_id, action, metadata)
VALUES (:admin_id, NULL, 'approve_access',
        jsonb_build_object('out_of_band', true, 'tenant_id', '<tenant>',
                           'evidence', '<ofício/email/...>'));
COMMIT;
```

---

## Revogar acesso

Para remover um vínculo aprovado:

```sql
BEGIN;
SET LOCAL app.tenant_id = '<tenant>';

DELETE FROM dashboard.user_tenants
WHERE user_id = '<user-uuid>' AND tenant_id = '<tenant>';

INSERT INTO dashboard.audit_log (user_id, tenant_id, action, metadata)
VALUES (:admin_id, NULL, 'reject_access',
        jsonb_build_object('revoke', true, 'user_id', '<user-uuid>',
                           'tenant_id', '<tenant>',
                           'reason', '<motivo>'));
COMMIT;
```

(Não há ação dedicada `revoke_access` na `chk_action` em v1.1 — reutiliza
`reject_access` com `metadata.revoke=true`.)

---

## Auditoria

Listar últimas decisões administrativas (qualquer tenant):

```sql
SELECT al.timestamp, u.email AS admin_email, al.action, al.metadata
FROM dashboard.audit_log al
JOIN dashboard.users u ON u.id = al.user_id
WHERE al.action IN ('approve_access', 'reject_access')
ORDER BY al.timestamp DESC
LIMIT 50;
```

---

## Erros comuns

- **`new row violates row-level security policy for table "user_tenants"`** —
  esqueceu `SET LOCAL app.tenant_id = '<tenant>'` ou setou tenant errado.
  Tenant de `app.tenant_id` precisa ser **idêntico** ao `tenant_id` que
  está sendo inserido.
- **`duplicate key value violates unique constraint
  "access_requests_user_id_tenant_id_key"`** — usuário já tem solicitação
  para esse tenant. Inspecionar `status`; se rejeitada e foi reaberta no
  canal externo, atualizar in-place ao invés de re-inserir.
- **`reviewed_by` NULL após approve/reject** — esqueceu `\set admin_id`
  ou variável psql não foi expandida. Sempre confirmar com `\echo
  :admin_id` antes do `UPDATE`.

## v1.2 (futuro)

UI admin substitui este runbook: tela `/admin/access-requests` (gated por
`role='admin'`), botões approve/reject, audit_log automático. Até lá,
manter este doc como referência canônica.
