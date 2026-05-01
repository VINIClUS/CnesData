import { useLocation } from "@tanstack/react-router";
import type { ReactNode } from "react";

import { Sidebar } from "./Sidebar";
import { TenantPill } from "./TenantPill";
import { ThemeToggle } from "./ThemeToggle";

import { useTenants } from "@/api/hooks/useTenants";
import { logout } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { t } from "@/i18n/pt-BR";

export function Shell({ children }: { children: ReactNode }) {
  const { user } = useAuth();
  const tenants = useTenants();
  const location = useLocation();
  const tenant = tenants.data?.[0];

  return (
    <div className="grid min-h-screen grid-cols-[14rem_1fr]">
      <Sidebar activePath={location.pathname} />
      <div className="flex flex-col">
        <header className="flex items-center justify-between border-b px-6 py-3">
          {tenant ? (
            <TenantPill nome={tenant.nome} uf={tenant.uf} ibge6={tenant.ibge6} />
          ) : (
            <span />
          )}
          <div className="flex items-center gap-3 text-xs text-muted-foreground">
            <ThemeToggle />
            <span>{user?.email}</span>
            <button type="button" onClick={() => void logout()} className="underline">
              {t.nav.sair}
            </button>
          </div>
        </header>
        <main className="flex-1 p-6">{children}</main>
      </div>
    </div>
  );
}
