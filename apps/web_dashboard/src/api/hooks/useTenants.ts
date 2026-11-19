import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import { isAwsMe } from "@/auth/types";
import { useAuth } from "@/auth/useAuth";
import { env } from "@/lib/env";

export type Tenant = {
  ibge6: string;
  ibge7: string;
  nome: string;
  uf: string;
};

export type Membership = { tenant_id: string; municipality_name: string; role: string };
export type AuthorizedMunicipality = Tenant | Membership;

export function tenantId(tenant: AuthorizedMunicipality): string {
  return "tenant_id" in tenant ? tenant.tenant_id : tenant.ibge6;
}

export function tenantName(tenant: AuthorizedMunicipality): string {
  return "municipality_name" in tenant ? tenant.municipality_name : tenant.nome;
}

export function useTenants() {
  const session = useAuth();
  const aws = session.user !== null && isAwsMe(session.user);
  return useQuery({
    queryKey: ["tenants", session.scope, aws],
    queryFn: ({ signal }) =>
      apiFetch<AuthorizedMunicipality[]>(aws ? "/dashboard/memberships" : "/dashboard/tenants", {
        signal,
        accessToken: session.accessToken,
      }),
    enabled: env.VITE_AUTH_MODE !== "local" && session.status === "authenticated",
    staleTime: 5 * 60_000,
  });
}

export function useSelectedTenant() {
  const session = useAuth();
  const tenants = useTenants();
  const selected =
    tenants.data?.find((tenant) => tenantId(tenant) === session.selectedTenantId) ??
    tenants.data?.[0];
  const selectTenant = (id: string) => {
    if (tenants.data?.some((tenant) => tenantId(tenant) === id)) session.selectTenant(id);
  };
  return { tenants, selected, selectTenant };
}
