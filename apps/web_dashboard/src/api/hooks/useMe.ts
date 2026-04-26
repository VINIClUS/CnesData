import { apiFetch } from "@/api/client";
import type { Me } from "@/auth/AuthProvider";
import { useQuery } from "@tanstack/react-query";

export function useMe() {
  return useQuery({
    queryKey: ["me"],
    queryFn: () => apiFetch<Me>("/dashboard/auth/me"),
    staleTime: 60_000,
  });
}
