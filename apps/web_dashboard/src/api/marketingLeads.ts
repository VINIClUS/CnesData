import { ApiError, apiFetch } from "./client";

import type { Lead } from "@/components/contact/leadSchema";

export const LEADS_PATH = "/public/leads";

export type LeadPayload = Lead & {
  municipality: string;
  source_path: string;
  source_cta: string;
  privacy_notice_version: "1";
  newsletter_opt_in: false;
};

export type LeadFailure = "validation" | "rate_limited" | "unavailable" | "network";
export type LeadResult = { ok: true } | { ok: false; reason: LeadFailure };
export const RETRYABLE_FAILURES: readonly LeadFailure[] = ["unavailable", "network"];

function reasonFor(status: number): LeadFailure {
  if (status === 422) return "validation";
  if (status === 429) return "rate_limited";
  return "unavailable";
}

export async function submitLead(payload: LeadPayload): Promise<LeadResult> {
  try {
    await apiFetch(LEADS_PATH, { method: "POST", body: JSON.stringify(payload) });
    return { ok: true };
  } catch (err) {
    if (err instanceof ApiError) return { ok: false, reason: reasonFor(err.status) };
    return { ok: false, reason: "network" };
  }
}
