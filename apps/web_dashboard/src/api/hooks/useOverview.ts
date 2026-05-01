import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";

export type OverviewKpis = {
  competencia_atual: number;
  faturamento_atual_cents: number;
  faturamento_anterior_cents: number;
  aih_atual: number;
  aih_anterior: number;
  profissionais_ativos: number;
  profissionais_anterior: number;
  estabs_sem_producao: number;
  estabs_total: number;
  estabs_sem_producao_anterior: number;
};

export function useOverview(tenantId: string) {
  return useQuery({
    queryKey: ["overview", tenantId],
    queryFn: () => apiFetch<OverviewKpis>("/dashboard/overview", { tenantId }),
    staleTime: 30_000,
    refetchInterval: 60_000,
  });
}
