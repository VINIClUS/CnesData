import { createFileRoute } from "@tanstack/react-router";

import { isServingUnavailable, useServingOverview } from "@/api/hooks/useServingOverview";
import { KpiGrid } from "@/components/overview/KpiGrid";

export const Route = createFileRoute("/_app/overview")({
  component: OverviewPage,
});

function OverviewPage() {
  const overview = useServingOverview();

  return (
    <section>
      <h1 className="mb-1 text-2xl font-semibold">Visão geral</h1>
      <p className="mb-6 text-xs text-muted-foreground">Reconciliação CNES da competência ativa.</p>
      {overview.isLoading && <p>Carregando...</p>}
      {overview.isError && isServingUnavailable(overview.error) && (
        <p className="text-sm text-red-600">Dados ainda não publicados para esta competência.</p>
      )}
      {overview.isError && !isServingUnavailable(overview.error) && (
        <p className="text-sm text-red-600">Erro ao carregar visão geral.</p>
      )}
      {overview.data && <OverviewContent overview={overview.data} />}
    </section>
  );
}

function OverviewContent({
  overview,
}: {
  overview: NonNullable<ReturnType<typeof useServingOverview>["data"]>;
}) {
  const divergenceEntries = Object.entries(overview.divergence_counts);
  return (
    <>
      <KpiGrid overview={overview} />
      {overview.missing_sources.length > 0 && (
        <div className="mt-6 rounded-lg border bg-card p-4">
          <h2 className="mb-1 text-sm font-semibold">Fontes ausentes</h2>
          <p className="text-xs text-muted-foreground">{overview.missing_sources.join(", ")}</p>
        </div>
      )}
      {divergenceEntries.length > 0 && (
        <div className="mt-6 rounded-lg border bg-card p-4">
          <h2 className="mb-1 text-sm font-semibold">Divergências por campo</h2>
          <ul className="text-xs text-muted-foreground">
            {divergenceEntries.map(([field, count]) => (
              <li key={field}>
                {field}: {count}
              </li>
            ))}
          </ul>
        </div>
      )}
    </>
  );
}
