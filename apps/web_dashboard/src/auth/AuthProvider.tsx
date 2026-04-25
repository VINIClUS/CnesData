import { type ReactNode, createContext, useCallback, useEffect, useState } from "react";

export type Me = {
  user_id: string;
  email: string;
  display_name: string | null;
  role: "gestor" | "admin";
  tenant_ids: string[];
};

export type AuthStatus = "loading" | "authenticated" | "anonymous";

export type AuthContextValue = {
  status: AuthStatus;
  user: Me | null;
  refresh: () => Promise<void>;
};

export const AuthContext = createContext<AuthContextValue | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [status, setStatus] = useState<AuthStatus>("loading");
  const [user, setUser] = useState<Me | null>(null);

  const refresh = useCallback(async () => {
    const r = await fetch("/api/v1/dashboard/auth/me", { credentials: "include" });
    if (r.ok) {
      const body = (await r.json()) as Me;
      setUser(body);
      setStatus("authenticated");
    } else {
      setUser(null);
      setStatus("anonymous");
    }
  }, []);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  return <AuthContext.Provider value={{ status, user, refresh }}>{children}</AuthContext.Provider>;
}
