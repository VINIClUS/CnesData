import { useMutation } from "@tanstack/react-query";

import { ApiError, apiFetch } from "@/api/client";

type ActivateRequest = { user_code: string; tenant_id: string };
type ActivateResponse = { status: "approved"; expires_in_seconds: number };

const ACTIVATE_FAILED = "activate_failed";

function errorDetail(body: unknown): string {
  const detail = typeof body === "object" && body !== null && "detail" in body ? body.detail : null;
  return typeof detail === "string" ? detail : ACTIVATE_FAILED;
}

function isApproved(body: unknown): body is ActivateResponse {
  return (
    typeof body === "object" && body !== null && "status" in body && body.status === "approved"
  );
}

async function postActivation(body: ActivateRequest): Promise<unknown> {
  try {
    return await apiFetch<unknown>("/activate/confirm", {
      method: "POST",
      tenantId: body.tenant_id,
      body: JSON.stringify(body),
    });
  } catch (error) {
    if (error instanceof ApiError) throw new Error(errorDetail(error.body));
    if (error instanceof SyntaxError) throw new Error(ACTIVATE_FAILED);
    throw error;
  }
}

async function confirmActivation(body: ActivateRequest): Promise<ActivateResponse> {
  const response = await postActivation(body);
  if (!isApproved(response)) throw new Error(ACTIVATE_FAILED);
  return response;
}

export function useActivate() {
  return useMutation<ActivateResponse, Error, ActivateRequest>({
    mutationFn: confirmActivation,
  });
}
