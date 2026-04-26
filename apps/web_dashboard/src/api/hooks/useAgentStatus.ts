import { apiFetch } from "@/api/client";
import { useQuery } from "@tanstack/react-query";

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
  return useQuery({
    queryKey: ["agent-status", tenantId],
    queryFn: () => apiFetch<AgentStatusResponse>("/dashboard/agents/status", { tenantId }),
    refetchInterval: 30_000,
    staleTime: 15_000,
  });
}
