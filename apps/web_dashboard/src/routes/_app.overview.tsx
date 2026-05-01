import { createFileRoute } from "@tanstack/react-router";

import { useFaturamentoChart } from "@/api/hooks/useFaturamentoChart";
import { useOverview } from "@/api/hooks/useOverview";
import { useTenants } from "@/api/hooks/useTenants";
import { FaturamentoAreaChart } from "@/components/overview/FaturamentoAreaChart";
import { KpiGrid } from "@/components/overview/KpiGrid";

export const Route = createFileRoute("/_app/overview")({
  component: OverviewPage,
});

function OverviewPage() {
  const tenants = useTenants();
  const tenantId = tenants.data?.[0]?.ibge6;

  if (!tenantId) return <p>Carregando...</p>;
  return <OverviewContent tenantId={tenantId} />;
}

function OverviewContent({ tenantId }: { tenantId: string }) {
  const overview = useOverview(tenantId);
  const chart = useFaturamentoChart(tenantId);

  return (
    <section>
      <h1 className="mb-1 text-2xl font-semibold">Visão geral</h1>
      <p className="mb-6 text-xs text-muted-foreground">
        KPIs e faturamento ambulatorial dos últimos 12 meses.
      </p>
      {overview.data ? <KpiGrid kpis={overview.data} /> : <p>Carregando...</p>}
      <div className="mt-8 rounded-lg border bg-card p-4">
        <h2 className="mb-1 text-sm font-semibold">Faturamento últimos 12 meses</h2>
        <p className="mb-3 text-xs text-muted-foreground">
          Top 10 estabelecimentos + bucket &quot;outros&quot;
        </p>
        {chart.data ? (
          <FaturamentoAreaChart data={chart.data} />
        ) : (
          <div className="h-[260px] animate-pulse rounded bg-muted" />
        )}
      </div>
    </section>
  );
}
