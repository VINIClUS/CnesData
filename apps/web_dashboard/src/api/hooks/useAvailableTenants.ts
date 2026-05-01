import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import type { Tenant } from "@/api/hooks/useTenants";

export function useAvailableTenants() {
  return useQuery({
    queryKey: ["available-tenants"],
    queryFn: () => apiFetch<Tenant[]>("/dashboard/access-requests/available-tenants"),
    staleTime: 60_000,
  });
}
