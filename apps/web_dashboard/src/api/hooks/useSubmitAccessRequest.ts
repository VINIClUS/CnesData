import { useMutation, useQueryClient } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";

type Request = { tenant_id: string; motivation: string };
type Response = { request_id: string };

export function useSubmitAccessRequest() {
  const qc = useQueryClient();
  return useMutation<Response, Error, Request>({
    mutationFn: (body) =>
      apiFetch<Response>("/dashboard/access-requests", {
        method: "POST",
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["access-requests-mine"] });
      void qc.invalidateQueries({ queryKey: ["available-tenants"] });
      void qc.invalidateQueries({ queryKey: ["me"] });
    },
  });
}
