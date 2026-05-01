import type { RunRow } from "@/api/hooks/useAgentRuns";
import { formatCompetenciaBR, formatRelativeTime } from "@/lib/format";

export function AgentRunsTable({ runs }: { runs: RunRow[] }) {
  return (
    <table className="w-full text-xs">
      <thead className="text-left text-muted-foreground">
        <tr>
          <th className="pb-2 pr-4 font-medium">quando</th>
          <th className="pb-2 pr-4 font-medium">fonte</th>
          <th className="pb-2 pr-4 font-medium">comp</th>
          <th className="pb-2 pr-4 font-medium">linhas</th>
          <th className="pb-2 pr-4 font-medium">sha256</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((r) => (
          <tr key={r.id} className="border-t">
            <td className="py-1 pr-4">{formatRelativeTime(new Date(r.extracao_ts))}</td>
            <td className="py-1 pr-4">{r.fonte_sistema}</td>
            <td className="py-1 pr-4">{formatCompetenciaBR(r.competencia)}</td>
            <td className="py-1 pr-4">{r.row_count.toLocaleString("pt-BR")}</td>
            <td className="py-1 pr-4 font-mono">{r.sha256.slice(0, 12)}…</td>
          </tr>
        ))}
        {runs.length === 0 && (
          <tr>
            <td colSpan={5} className="py-2 text-muted-foreground">
              Sem execuções recentes.
            </td>
          </tr>
        )}
      </tbody>
    </table>
  );
}
