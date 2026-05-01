import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import type { Me } from "@/auth/AuthProvider";

export function useMe() {
  return useQuery({
    queryKey: ["me"],
    queryFn: () => apiFetch<Me>("/dashboard/auth/me"),
    staleTime: 60_000,
  });
}
