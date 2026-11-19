import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import { useAuth } from "@/auth/useAuth";

export type SourceStatus = {
  fonte_sistema: "CNES_LOCAL" | "CNES_NACIONAL" | "SIHD" | "BPA_MAG" | "SIA_LOCAL";
  last_extracao_ts: string | null;
  last_competencia: number | null;
  lag_months: number | null;
  row_count: number | null;
  status: "ok" | "warning" | "error" | "no_data";
  last_machine_id: string | null;
};

export type AgentStatusResponse = {
  fetched_at: string;
  sources: SourceStatus[];
};

export function useAgentStatus(tenantId: string) {
  const session = useAuth();
  return useQuery({
    queryKey: ["agent-status", session.scope, tenantId],
    queryFn: ({ signal }) =>
      apiFetch<AgentStatusResponse>("/dashboard/agents/status", {
        tenantId,
        signal,
        accessToken: session.accessToken,
      }),
    enabled: session.status === "authenticated",
    refetchInterval: 30_000,
    staleTime: 15_000,
  });
}
