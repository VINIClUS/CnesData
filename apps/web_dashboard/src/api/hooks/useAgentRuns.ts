import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import { useAuth } from "@/auth/useAuth";

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
  const session = useAuth();
  return useQuery({
    queryKey: ["agent-runs", session.scope, tenantId, limit],
    queryFn: ({ signal }) =>
      apiFetch<AgentRunsResponse>(`/dashboard/agents/runs?limit=${limit}`, {
        tenantId,
        signal,
        accessToken: session.accessToken,
      }),
    enabled: session.status === "authenticated",
    refetchInterval: 30_000,
    staleTime: 15_000,
  });
}
