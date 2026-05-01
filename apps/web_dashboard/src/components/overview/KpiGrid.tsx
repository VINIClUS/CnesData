import { KpiCard } from "./KpiCard";

import type { OverviewKpis } from "@/api/hooks/useOverview";
import { formatBRL, formatCompetenciaBR } from "@/lib/format";

function _previousCompetencia(yyyymm: number): number {
  const y = Math.floor(yyyymm / 100);
  const m = yyyymm % 100;
  if (m === 1) return (y - 1) * 100 + 12;
  return y * 100 + (m - 1);
}

function _deltaPct(cur: number, prev: number) {
  if (prev === 0) return undefined;
  const pct = ((cur - prev) / prev) * 100;
  if (Math.abs(pct) < 0.05) return undefined;
  return {
    value: `${pct.toFixed(1)}%`,
    direction: pct >= 0 ? ("up" as const) : ("down" as const),
  };
}

function _deltaCount(cur: number, prev: number, warnOnIncrease = false) {
  const diff = cur - prev;
  if (diff === 0) return undefined;
  const dir = warnOnIncrease
    ? diff > 0
      ? ("warn" as const)
      : ("down" as const)
    : diff > 0
      ? ("up" as const)
      : ("down" as const);
  return { value: `${Math.abs(diff)}`, direction: dir };
}

export function KpiGrid({ kpis }: { kpis: OverviewKpis }) {
  const prevComp = formatCompetenciaBR(_previousCompetencia(kpis.competencia_atual));
  const compStr = formatCompetenciaBR(kpis.competencia_atual);
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
      <KpiCard
        label={`Faturamento ${compStr}`}
        value={formatBRL(kpis.faturamento_atual_cents)}
        delta={_deltaPct(kpis.faturamento_atual_cents, kpis.faturamento_anterior_cents)}
        context={`vs ${prevComp}`}
      />
      <KpiCard
        label={`AIHs ${compStr}`}
        value={kpis.aih_atual.toLocaleString("pt-BR")}
        delta={_deltaPct(kpis.aih_atual, kpis.aih_anterior)}
        context={`vs ${prevComp}`}
      />
      <KpiCard
        label="Profissionais ativos"
        value={kpis.profissionais_ativos.toLocaleString("pt-BR")}
        delta={_deltaCount(kpis.profissionais_ativos, kpis.profissionais_anterior)}
        context={`vs ${prevComp}`}
      />
      <KpiCard
        label="Estabs sem produção"
        value={`${kpis.estabs_sem_producao}`}
        delta={_deltaCount(kpis.estabs_sem_producao, kpis.estabs_sem_producao_anterior, true)}
        context={`de ${kpis.estabs_total} totais`}
      />
    </div>
  );
}
