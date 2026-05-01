import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";

export type FaturamentoSeriesPoint = {
  competencia: string;
  [estab: string]: string | number;
};

export type FaturamentoChart = {
  series: FaturamentoSeriesPoint[];
  categories: string[];
};

export function useFaturamentoChart(tenantId: string, months = 12) {
  return useQuery({
    queryKey: ["faturamento-chart", tenantId, months],
    queryFn: () =>
      apiFetch<FaturamentoChart>(`/dashboard/faturamento/by-establishment?months=${months}`, {
        tenantId,
      }),
    staleTime: 60_000,
  });
}
