import { KpiCard } from "./KpiCard";

import type { ServingOverview } from "@/api/hooks/useServingOverview";

function _competenciaBR(competencia: string): string {
  const [year, month] = competencia.split("-");
  return `${month}/${year}`;
}

export function KpiGrid({ overview }: { overview: ServingOverview }) {
  const { kpis } = overview;
  const compStr = _competenciaBR(overview.competencia);
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-4">
      <KpiCard
        label={`Correspondências ${compStr}`}
        value={kpis.match_count.toLocaleString("pt-BR")}
        context="local × nacional"
      />
      <KpiCard
        label="Divergências"
        value={kpis.conflict_count.toLocaleString("pt-BR")}
        context={`${kpis.reconciled_row_count.toLocaleString("pt-BR")} registros reconciliados`}
      />
      <KpiCard
        label="Profissionais ativos"
        value={kpis.active_professional_count.toLocaleString("pt-BR")}
      />
      <KpiCard
        label="Só local / só nacional"
        value={`${kpis.local_only_count.toLocaleString("pt-BR")} / ${kpis.national_only_count.toLocaleString("pt-BR")}`}
      />
    </div>
  );
}
