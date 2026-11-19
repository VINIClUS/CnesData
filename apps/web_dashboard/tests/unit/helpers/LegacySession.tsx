import type { ReactNode } from "react";

import { AuthContext, type AuthContextValue } from "@/auth/AuthProvider";

export const LEGACY_SCOPE = ["legacy", "u-1", 1] as const;
const session: AuthContextValue = {
  status: "authenticated",
  user: {
    user_id: "u-1",
    email: "g@m",
    display_name: null,
    role: "gestor",
    tenant_ids: ["354130"],
    has_pending_request: false,
  },
  scope: LEGACY_SCOPE,
  accessToken: "tok",
  selectedTenantId: null,
  selectTenant: () => undefined,
  refresh: () => Promise.resolve(),
};

export function LegacySession({ children }: { children: ReactNode }) {
  return <AuthContext.Provider value={session}>{children}</AuthContext.Provider>;
}
