import { useContext } from "react";

import { AuthContext, type AuthContextValue } from "@/auth/AuthProvider";

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
