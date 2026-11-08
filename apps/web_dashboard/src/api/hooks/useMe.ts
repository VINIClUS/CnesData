import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import type { Me } from "@/auth/types";
import { useAuth } from "@/auth/useAuth";

export function useMe() {
  const session = useAuth();
  return useQuery({
    queryKey: ["me", session.scope],
    queryFn: ({ signal }) =>
      apiFetch<Me>("/dashboard/auth/me", { signal, accessToken: session.accessToken }),
    enabled: session.status === "authenticated",
    staleTime: 60_000,
  });
}
