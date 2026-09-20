import { useQuery } from "@tanstack/react-query";

import { ApiError, apiFetch } from "@/api/client";

export type ServingKpis = {
  match_count: number;
  local_only_count: number;
  national_only_count: number;
  conflict_count: number;
  reconciled_row_count: number;
  active_professional_count: number;
};

export type ServingOverview = {
  schema_version: string;
  tenant_id: string;
  run_id: string;
  generated_at: string;
  competencia: string;
  kpis: ServingKpis;
  divergence_counts: Record<string, number>;
  missing_sources: string[];
};

const SERVING_OVERVIEW_PATH = "/dashboard/serving/cnes/overview";

export function isServingUnavailable(error: unknown): boolean {
  if (!(error instanceof ApiError)) return false;
  const body = error.body as { detail?: string } | null;
  return error.status === 503 && body?.detail === "active_serving_unavailable";
}

export function useServingOverview() {
  return useQuery({
    queryKey: ["serving-overview"],
    queryFn: () => {
      const options: RequestInit & { tenantId?: never } = {};
      return apiFetch<ServingOverview>(SERVING_OVERVIEW_PATH, options);
    },
    staleTime: 30_000,
  });
}
