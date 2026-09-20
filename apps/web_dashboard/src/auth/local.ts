import { apiFetch } from "@/api/client";
import type { LocalPrincipal } from "@/auth/types";

export const LOCAL_AUTH_ME_PATH = "/auth/me";

export function loginLocal(email: string, password: string): Promise<LocalPrincipal> {
  return apiFetch<LocalPrincipal>("/auth/local/login", {
    method: "POST",
    body: JSON.stringify({ email, password }),
  });
}

export function getLocalPrincipal(): Promise<LocalPrincipal> {
  return apiFetch<LocalPrincipal>(LOCAL_AUTH_ME_PATH);
}

export function logoutLocal(): Promise<null> {
  return apiFetch<null>("/auth/logout", { method: "POST" });
}
