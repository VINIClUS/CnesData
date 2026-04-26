import { createFileRoute } from "@tanstack/react-router";

import { useAgentRuns } from "@/api/hooks/useAgentRuns";
import { useAgentStatus } from "@/api/hooks/useAgentStatus";
import { useTenants } from "@/api/hooks/useTenants";
import { AgentRunsTable } from "@/components/agentes/AgentRunsTable";
import { AgentStatusCard } from "@/components/agentes/AgentStatusCard";

export const Route = createFileRoute("/_app/agentes")({
  component: AgentesPage,
});

function AgentesPage() {
  const tenants = useTenants();
  const tenantId = tenants.data?.[0]?.ibge6;

  if (!tenantId) {
    return <p>Carregando...</p>;
  }
  return <AgentesContent tenantId={tenantId} />;
}

function AgentesContent({ tenantId }: { tenantId: string }) {
  const status = useAgentStatus(tenantId);
  const runs = useAgentRuns(tenantId);

  return (
    <section>
      <h1 className="mb-1 text-2xl font-semibold">Status dos agentes</h1>
      <p className="mb-4 text-xs text-muted-foreground">
        Auto-refresh 30s
        {status.dataUpdatedAt > 0 &&
          ` · ${new Date(status.dataUpdatedAt).toLocaleTimeString("pt-BR")}`}
      </p>
      <div className="mb-8 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {status.data?.sources.map((s) => (
          <AgentStatusCard key={s.fonte_sistema} data={s} />
        ))}
      </div>
      <h2 className="mb-2 text-sm font-semibold">Últimas 20 execuções</h2>
      {runs.data ? <AgentRunsTable runs={runs.data.runs} /> : <p>Carregando...</p>}
    </section>
  );
}
