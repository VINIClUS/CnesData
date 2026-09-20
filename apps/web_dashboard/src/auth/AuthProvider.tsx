import { type ReactNode, createContext, useCallback, useEffect, useState } from "react";

import { apiFetch } from "@/api/client";
import { getLocalPrincipal } from "@/auth/local";
import { getAccessToken } from "@/auth/oidc";
import type { Me } from "@/auth/types";
import { env } from "@/lib/env";

export type { Me } from "@/auth/types";

export type AuthStatus = "loading" | "authenticated" | "anonymous";

export type AuthContextValue = {
  status: AuthStatus;
  user: Me | null;
  refresh: () => Promise<void>;
};

// eslint-disable-next-line react-refresh/only-export-components
export const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<Me | null>(null);

  const refresh = useCallback(async () => {
    try {
      if (env.VITE_AUTH_MODE === "local") {
        const principal = await getLocalPrincipal();
        setUser({
          user_id: principal.user_id,
          email: principal.email,
          display_name: null,
          role: principal.role,
          tenant_ids: [principal.tenant_id],
          has_pending_request: false,
        });
        setStatus("authenticated");
        return;
      }
      const token = await getAccessToken();
      if (!token) {
        setUser(null);
        setStatus("anonymous");
        return;
      }
      const body = await apiFetch<Me>("/dashboard/auth/me");
      setUser(body);
      setStatus("authenticated");
      return;
    } catch {
      /* network failure treated as anonymous */
    }
    setUser(null);
    setStatus("anonymous");
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return <AuthContext.Provider value={{ status, user, refresh }}>{children}</AuthContext.Provider>;
}
