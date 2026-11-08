import { useMutation, useQueryClient } from "@tanstack/react-query";

import { apiFetch } from "@/api/client";
import { useAuth } from "@/auth/useAuth";

type Request = { tenant_id: string; motivation: string };
type Response = { request_id: string };

export function useSubmitAccessRequest() {
  const qc = useQueryClient();
  const session = useAuth();
  return useMutation<Response, Error, Request>({
    mutationFn: (body) =>
      apiFetch<Response>("/dashboard/access-requests", {
        method: "POST",
        body: JSON.stringify(body),
        accessToken: session.accessToken,
      }),
    onSuccess: () => {
      void qc.invalidateQueries({ queryKey: ["access-requests-mine", session.scope] });
      void qc.invalidateQueries({ queryKey: ["available-tenants", session.scope] });
      void qc.invalidateQueries({ queryKey: ["me", session.scope] });
    },
  });
}
