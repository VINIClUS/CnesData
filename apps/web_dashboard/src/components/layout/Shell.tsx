import { useLocation, useNavigate } from "@tanstack/react-router";
import type { ReactNode } from "react";

import { Sidebar } from "./Sidebar";
import { TenantPill } from "./TenantPill";
import { ThemeToggle } from "./ThemeToggle";

import { useTenants } from "@/api/hooks/useTenants";
import { logoutLocal } from "@/auth/local";
import { logout } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { t } from "@/i18n/pt-BR";
import { env } from "@/lib/env";

export function Shell({ children }: { children: ReactNode }) {
  const { user, refresh } = useAuth();
  const tenants = useTenants();
  const location = useLocation();
  const navigate = useNavigate();
  const tenant = tenants.data?.[0];
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
          {tenant ? (
            <TenantPill nome={tenant.nome} uf={tenant.uf} ibge6={tenant.ibge6} />
          ) : (
            <span />
          )}
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
