"""Overview KPIs + faturamento chart aggregation for DashboardRepo."""

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass
class OverviewKpis:
    competencia_atual: int
    faturamento_atual_cents: int
    faturamento_anterior_cents: int
    aih_atual: int
    aih_anterior: int
    profissionais_ativos: int
    profissionais_anterior: int
    estabs_sem_producao: int
    estabs_total: int
    estabs_sem_producao_anterior: int


@dataclass
class FaturamentoChart:
    series: list[dict[str, str | int]]
    categories: list[str]


def _previous_competencia(yyyymm: int) -> int:
    y, m = divmod(yyyymm, 100)
    if m == 1:
        return (y - 1) * 100 + 12
    return y * 100 + (m - 1)


def _format_competencia(yyyymm: int) -> str:
    months = ["jan", "fev", "mar", "abr", "mai", "jun",
              "jul", "ago", "set", "out", "nov", "dez"]
    y, m = divmod(yyyymm, 100)
    return f"{months[m - 1]}/{y}"


_FATURAMENTO_KPI_SQL = text("""
    SELECT COALESCE(SUM(f.valor_aprov_cents), 0)::BIGINT AS total
    FROM gold.fato_producao_ambulatorial f
    JOIN gold.dim_competencia c ON c.sk_competencia = f.sk_competencia
    JOIN gold.dim_estabelecimento e ON e.sk_estabelecimento = f.sk_estabelecimento
    WHERE c.competencia = :comp
      AND e.sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
""")


_AIH_KPI_SQL = text("""
    SELECT COUNT(*)::INT AS total
    FROM gold.fato_internacao f
    JOIN gold.dim_competencia c ON c.sk_competencia = f.sk_competencia
    JOIN gold.dim_estabelecimento e ON e.sk_estabelecimento = f.sk_estabelecimento
    WHERE c.competencia = :comp
      AND e.sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
""")


_PROFISSIONAIS_KPI_SQL = text("""
    SELECT COUNT(DISTINCT f.sk_profissional)::INT AS total
    FROM gold.fato_vinculo_cnes f
    JOIN gold.dim_competencia c ON c.sk_competencia = f.sk_competencia
    JOIN gold.dim_estabelecimento e ON e.sk_estabelecimento = f.sk_estabelecimento
    WHERE c.competencia = :comp
      AND e.sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
""")


_ESTABS_TOTAL_SQL = text("""
    SELECT COUNT(*)::INT AS total
    FROM gold.dim_estabelecimento
    WHERE sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
""")


_ESTABS_SEM_PRODUCAO_SQL = text("""
    SELECT COUNT(*)::INT AS total
    FROM gold.dim_estabelecimento e
    WHERE e.sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
      AND NOT EXISTS (
        SELECT 1 FROM gold.fato_producao_ambulatorial f
        JOIN gold.dim_competencia c ON c.sk_competencia = f.sk_competencia
        WHERE f.sk_estabelecimento = e.sk_estabelecimento
          AND c.competencia = :comp
      )
""")


_TOP_ESTABS_SQL = text("""
    SELECT e.sk_estabelecimento, e.nome
    FROM gold.fato_producao_ambulatorial f
    JOIN gold.dim_competencia c ON c.sk_competencia = f.sk_competencia
    JOIN gold.dim_estabelecimento e ON e.sk_estabelecimento = f.sk_estabelecimento
    WHERE c.competencia = ANY(:comps)
      AND e.sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
    GROUP BY e.sk_estabelecimento, e.nome
    ORDER BY SUM(f.valor_aprov_cents) DESC
    LIMIT 10
""")


_FATURAMENTO_PIVOT_SQL = text("""
    SELECT c.competencia, e.sk_estabelecimento,
           SUM(f.valor_aprov_cents)::BIGINT AS valor
    FROM gold.fato_producao_ambulatorial f
    JOIN gold.dim_competencia c ON c.sk_competencia = f.sk_competencia
    JOIN gold.dim_estabelecimento e ON e.sk_estabelecimento = f.sk_estabelecimento
    WHERE c.competencia = ANY(:comps)
      AND e.sk_municipio = (SELECT sk_municipio FROM gold.dim_municipio WHERE ibge6 = :tenant)
    GROUP BY c.competencia, e.sk_estabelecimento
""")


def overview_kpis_query(
    engine: Engine, *, tenant_id: str, current_competencia: int,
) -> OverviewKpis:
    prev = _previous_competencia(current_competencia)
    with engine.connect() as conn:
        params_cur = {"comp": current_competencia, "tenant": tenant_id}
        params_prev = {"comp": prev, "tenant": tenant_id}
        params_t = {"tenant": tenant_id}
        fat_cur = conn.execute(_FATURAMENTO_KPI_SQL, params_cur).scalar_one()
        fat_prev = conn.execute(_FATURAMENTO_KPI_SQL, params_prev).scalar_one()
        aih_cur = conn.execute(_AIH_KPI_SQL, params_cur).scalar_one()
        aih_prev = conn.execute(_AIH_KPI_SQL, params_prev).scalar_one()
        prof_cur = conn.execute(_PROFISSIONAIS_KPI_SQL, params_cur).scalar_one()
        prof_prev = conn.execute(_PROFISSIONAIS_KPI_SQL, params_prev).scalar_one()
        estabs_total = conn.execute(_ESTABS_TOTAL_SQL, params_t).scalar_one()
        sem_cur = conn.execute(_ESTABS_SEM_PRODUCAO_SQL, params_cur).scalar_one()
        sem_prev = conn.execute(_ESTABS_SEM_PRODUCAO_SQL, params_prev).scalar_one()
    return OverviewKpis(
        competencia_atual=current_competencia,
        faturamento_atual_cents=int(fat_cur),
        faturamento_anterior_cents=int(fat_prev),
        aih_atual=int(aih_cur),
        aih_anterior=int(aih_prev),
        profissionais_ativos=int(prof_cur),
        profissionais_anterior=int(prof_prev),
        estabs_sem_producao=int(sem_cur),
        estabs_total=int(estabs_total),
        estabs_sem_producao_anterior=int(sem_prev),
    )


def faturamento_by_establishment_query(
    engine: Engine, *, tenant_id: str, months: int, current_competencia: int,
) -> FaturamentoChart:
    comps: list[int] = []
    c = current_competencia
    for _ in range(months):
        comps.append(c)
        c = _previous_competencia(c)
    comps = list(reversed(comps))
    with engine.connect() as conn:
        top = conn.execute(
            _TOP_ESTABS_SQL, {"comps": comps, "tenant": tenant_id},
        ).mappings().all()
        rows = conn.execute(
            _FATURAMENTO_PIVOT_SQL, {"comps": comps, "tenant": tenant_id},
        ).mappings().all()
    return _build_faturamento_chart(comps, list(top), list(rows))


def _build_faturamento_chart(
    comps: list[int],
    top: list[dict],
    rows: list[dict],
) -> FaturamentoChart:
    top_ids = [r["sk_estabelecimento"] for r in top]
    top_names = [r["nome"] for r in top]
    series: list[dict[str, str | int]] = []
    for comp in comps:
        point: dict[str, str | int] = {"competencia": _format_competencia(comp)}
        outros = 0
        for r in rows:
            if r["competencia"] != comp:
                continue
            if r["sk_estabelecimento"] in top_ids:
                idx = top_ids.index(r["sk_estabelecimento"])
                point[top_names[idx]] = int(r["valor"])
            else:
                outros += int(r["valor"])
        for name in top_names:
            point.setdefault(name, 0)
        point["outros"] = outros
        series.append(point)
    categories = [*top_names, "outros"]
    return FaturamentoChart(series=series, categories=categories)
