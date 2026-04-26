import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";

export type RunRow = {
  id: string;
  extracao_ts: string;
  fonte_sistema: string;
  competencia: number;
  row_count: number;
  sha256: string;
  machine_id: string | null;
};

export type AgentRunsResponse = { runs: RunRow[] };

export function useAgentRuns(tenantId: string, limit = 20) {
  return useQuery({
    queryKey: ["agent-runs", tenantId, limit],
    queryFn: () =>
      apiFetch<AgentRunsResponse>(`/dashboard/agents/runs?limit=${limit}`, { tenantId }),
    refetchInterval: 30_000,
    staleTime: 15_000,
  });
}
