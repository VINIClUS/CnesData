import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import { isAwsMe } from "@/auth/types";
import { useAuth } from "@/auth/useAuth";

export type AccessRequest = {
  id: string;
  tenant_id: string;
  tenant_nome: string | null;
  motivation: string;
  status: "pending" | "approved" | "rejected";
  requested_at: string;
  reviewed_at: string | null;
  review_notes: string | null;
};

export function useAccessRequests() {
  const session = useAuth();
  return useQuery({
    queryKey: ["access-requests-mine", session.scope],
    queryFn: ({ signal }) =>
      apiFetch<AccessRequest[]>("/dashboard/access-requests/mine", {
        signal,
        accessToken: session.accessToken,
      }),
    enabled: session.status === "authenticated" && session.user !== null && !isAwsMe(session.user),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });
}
