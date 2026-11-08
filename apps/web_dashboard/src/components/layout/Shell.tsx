import { useLocation, useNavigate } from "@tanstack/react-router";
import type { ReactNode } from "react";

import { Sidebar } from "./Sidebar";
import { TenantPill } from "./TenantPill";
import { ThemeToggle } from "./ThemeToggle";

import { tenantId, tenantName, useSelectedTenant } from "@/api/hooks/useTenants";
import { logoutLocal } from "@/auth/local";
import { logout } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { t } from "@/i18n/pt-BR";
import { env } from "@/lib/env";

export function Shell({ children }: { children: ReactNode }) {
  const { user, refresh } = useAuth();
  const location = useLocation();
  const navigate = useNavigate();
  const signOut = async () => {
    if (env.VITE_AUTH_MODE === "local") {
      await logoutLocal();
      await refresh();
      await navigate({ to: "/login" });
      return;
    }
    await logout();
  };

  return (
    <div className="grid min-h-screen grid-cols-[14rem_1fr]">
      <Sidebar activePath={location.pathname} />
      <div className="flex flex-col">
        <header className="flex items-center justify-between border-b px-6 py-3">
          <TenantSelector />
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <ThemeToggle />
            <span>{user?.email}</span>
            <button type="button" onClick={() => void signOut()} className="underline">
              {t.nav.sair}
            </button>
          </div>
        </header>
        <main className="flex-1 p-6">{children}</main>
      </div>
    </div>
  );
}

function TenantSelector() {
  const { tenants, selected: tenant, selectTenant } = useSelectedTenant();
  if (tenants.isError) return <span role="alert">Não foi possível consultar os municípios.</span>;
  if (!tenant) return <span />;
  if ((tenants.data?.length ?? 0) > 1) {
    return (
      <select
        aria-label="Município"
        value={tenantId(tenant)}
        onChange={(event) => selectTenant(event.target.value)}
      >
        {tenants.data?.map((item) => (
          <option key={tenantId(item)} value={tenantId(item)}>
            {tenantName(item)}
          </option>
        ))}
      </select>
    );
  }
  return "ibge6" in tenant ? (
    <TenantPill nome={tenant.nome} uf={tenant.uf} ibge6={tenant.ibge6} />
  ) : (
    <span>{tenant.municipality_name}</span>
  );
}
