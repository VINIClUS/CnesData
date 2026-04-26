import type { SourceStatus } from "@/api/hooks/useAgentStatus";
import { formatCompetenciaBR, formatLagMeses, formatRelativeTime } from "@/lib/format";

const _COLOR: Record<SourceStatus["status"], string> = {
  ok: "bg-green-500",
  warning: "bg-yellow-500",
  error: "bg-red-500",
  no_data: "bg-gray-400",
};

export function AgentStatusCard({ data }: { data: SourceStatus }) {
  const ts = data.last_extracao_ts ? new Date(data.last_extracao_ts) : null;
  return (
    <article className="rounded-lg border bg-card p-4">
      <header className="flex items-center gap-2 text-sm font-semibold">
        <span className={`inline-block h-2 w-2 rounded-full ${_COLOR[data.status]}`} aria-hidden />
        {data.fonte_sistema}
      </header>
      <dl className="mt-2 space-y-1 text-xs text-muted-foreground">
        {ts && data.last_competencia !== null ? (
          <>
            <div>
              comp {formatCompetenciaBR(data.last_competencia)} · {formatRelativeTime(ts)}
            </div>
            <div>{(data.row_count ?? 0).toLocaleString("pt-BR")} linhas</div>
            <div>{formatLagMeses(data.lag_months ?? 0)}</div>
          </>
        ) : (
          <div>sem extração</div>
        )}
        {data.last_machine_id && <div>edge: {data.last_machine_id}</div>}
      </dl>
    </article>
  );
}
