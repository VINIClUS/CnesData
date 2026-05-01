import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";

export type Tenant = {
  ibge6: string;
  ibge7: string;
  nome: string;
  uf: string;
};

export function useTenants() {
  return useQuery({
    queryKey: ["tenants"],
    queryFn: () => apiFetch<Tenant[]>("/dashboard/tenants"),
    staleTime: 5 * 60_000,
  });
}
