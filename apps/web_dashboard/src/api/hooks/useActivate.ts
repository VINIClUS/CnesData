import { useMutation } from "@tanstack/react-query";

import { getAccessToken } from "@/auth/oidc";

type ActivateRequest = { user_code: string; tenant_id: string };
type ActivateResponse = { status: "approved"; expires_in_seconds: number };

export function useActivate() {
  return useMutation<ActivateResponse, Error, ActivateRequest>({
    mutationFn: async (body) => {
      const token = await getAccessToken();
      const res = await fetch("/activate/confirm", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
          "X-Tenant-Id": body.tenant_id,
        },
        body: JSON.stringify(body),
      });
      const data = (await res.json()) as { detail?: string } & Partial<ActivateResponse>;
      if (!res.ok) {
        throw new Error(data.detail ?? "activate_failed");
      }
      return data as ActivateResponse;
    },
  });
}
