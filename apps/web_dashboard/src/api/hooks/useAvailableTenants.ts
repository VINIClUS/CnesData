import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import type { Tenant } from "@/api/hooks/useTenants";
import { isAwsMe } from "@/auth/types";
import { useAuth } from "@/auth/useAuth";

export function useAvailableTenants() {
  const session = useAuth();
  return useQuery({
    queryKey: ["available-tenants", session.scope],
    queryFn: ({ signal }) =>
      apiFetch<Tenant[]>("/dashboard/access-requests/available-tenants", {
        signal,
        accessToken: session.accessToken,
      }),
    enabled: session.status === "authenticated" && session.user !== null && !isAwsMe(session.user),
    staleTime: 60_000,
  });
}
