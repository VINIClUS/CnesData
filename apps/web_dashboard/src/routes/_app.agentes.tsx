import { useAgentRuns } from "@/api/hooks/useAgentRuns";
import { useAgentStatus } from "@/api/hooks/useAgentStatus";
import { useTenants } from "@/api/hooks/useTenants";
import { AgentRunsTable } from "@/components/agentes/AgentRunsTable";
import { AgentStatusCard } from "@/components/agentes/AgentStatusCard";
import { createFileRoute } from "@tanstack/react-router";

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
      <h1 className="text-2xl font-semibold mb-1">Status dos agentes</h1>
      <p className="text-xs text-muted-foreground mb-4">
        Auto-refresh 30s
        {status.dataUpdatedAt > 0 &&
          ` · ${new Date(status.dataUpdatedAt).toLocaleTimeString("pt-BR")}`}
      </p>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3 mb-8">
        {status.data?.sources.map((s) => (
          <AgentStatusCard key={s.fonte_sistema} data={s} />
        ))}
      </div>
      <h2 className="text-sm font-semibold mb-2">Últimas 20 execuções</h2>
      {runs.data ? <AgentRunsTable runs={runs.data.runs} /> : <p>Carregando...</p>}
    </section>
  );
}
