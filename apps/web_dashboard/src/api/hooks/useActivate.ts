import { useMutation } from "@tanstack/react-query";

import { ApiError, apiFetch } from "@/api/client";

type ActivateRequest = { user_code: string; tenant_id: string };
type ActivateResponse = { status: "approved"; expires_in_seconds: number };

function errorDetail(body: unknown): string {
  const detail = typeof body === "object" && body !== null && "detail" in body ? body.detail : null;
  return typeof detail === "string" ? detail : "activate_failed";
}

async function confirmActivation(body: ActivateRequest): Promise<ActivateResponse> {
  try {
    return await apiFetch<ActivateResponse>("/activate/confirm", {
      method: "POST",
      tenantId: body.tenant_id,
      body: JSON.stringify(body),
    });
  } catch (error) {
    if (error instanceof ApiError) throw new Error(errorDetail(error.body));
    throw error;
  }
}

export function useActivate() {
  return useMutation<ActivateResponse, Error, ActivateRequest>({
    mutationFn: confirmActivation,
  });
}
