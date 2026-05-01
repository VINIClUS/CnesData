import { useQuery } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";

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
  return useQuery({
    queryKey: ["access-requests-mine"],
    queryFn: () => apiFetch<AccessRequest[]>("/dashboard/access-requests/mine"),
    staleTime: 30_000,
    refetchInterval: 30_000,
  });
}
