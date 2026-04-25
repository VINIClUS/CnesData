import { AuthContext, type AuthContextValue } from "@/auth/AuthProvider";
import { useContext } from "react";

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be inside AuthProvider");
  return ctx;
}
