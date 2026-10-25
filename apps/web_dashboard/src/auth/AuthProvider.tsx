import { type ReactNode, createContext, useCallback, useEffect, useRef, useState } from "react";

import { apiFetch } from "@/api/client";
import { getLocalPrincipal } from "@/auth/local";
import { getAccessToken } from "@/auth/oidc";
import { SESSION_CHANGED } from "@/auth/sessionEvents";
import { type Me, principalKey } from "@/auth/types";
import { env } from "@/lib/env";

export type { Me } from "@/auth/types";

export type AuthStatus = "loading" | "authenticated" | "anonymous";

export type AuthContextValue = {
  status: AuthStatus;
  user: Me | null;
  refresh: () => Promise<void>;
  scope: readonly (string | number)[];
  accessToken: string | null;
  selectedTenantId: string | null;
  selectTenant: (tenantId: string) => void;
};

// eslint-disable-next-line react-refresh/only-export-components
export const AuthContext = createContext<AuthContextValue | null>(null);

type Session = { status: AuthStatus; user: Me | null; token: string | null; version: number };

async function loadSession(): Promise<Pick<Session, "user" | "token">> {
  if (env.VITE_AUTH_MODE === "local") {
    const principal = await getLocalPrincipal();
    return {
      token: null,
      user: {
        user_id: principal.user_id,
        email: principal.email,
        display_name: null,
        role: principal.role,
        tenant_ids: [principal.tenant_id],
        has_pending_request: false,
      },
    };
  }
  const token = await getAccessToken();
  return {
    token,
    user: token ? await apiFetch<Me>("/dashboard/auth/me", { accessToken: token }) : null,
  };
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<Session>({
    status: "loading",
    user: null,
    token: null,
    version: 0,
  });
  const [selectedTenantId, setSelectedTenantId] = useState<string | null>(null);
  const version = useRef(0);

  const refresh = useCallback(async () => {
    const current = ++version.current;
    setSelectedTenantId(null);
    setSession({ status: "loading", user: null, token: null, version: current });
    try {
      const loaded = await loadSession();
      if (current !== version.current) return;
      setSession({
        ...loaded,
        status: loaded.user ? "authenticated" : "anonymous",
        version: current,
      });
    } catch {
      if (current === version.current) {
        setSession({ status: "anonymous", user: null, token: null, version: current });
      }
    }
  }, []);

  useSessionEvents(refresh, version);

  const value: AuthContextValue = {
    status: session.status,
    user: session.user,
    refresh,
    scope: [...principalKey(session.user), session.version],
    accessToken: session.token,
    selectedTenantId,
    selectTenant: setSelectedTenantId,
  };
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

function useSessionEvents(refresh: () => Promise<void>, version: { current: number }) {
  useEffect(() => {
    const onSessionChange = () => {
      void refresh();
    };
    window.addEventListener(SESSION_CHANGED, onSessionChange);
    void refresh();
    const requests = version;
    return () => {
      window.removeEventListener(SESSION_CHANGED, onSessionChange);
      requests.current++;
    };
  }, [refresh, version]);
}
