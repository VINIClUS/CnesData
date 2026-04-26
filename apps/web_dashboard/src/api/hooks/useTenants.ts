import { apiFetch } from "@/api/client";
import { useQuery } from "@tanstack/react-query";

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
