import { type UseQueryResult, useQuery } from "@tanstack/react-query";

import { ApiError, apiFetch } from "@/api/client";
import {
  type AuthorizedMunicipality,
  tenantId as municipalityId,
  useSelectedTenant,
} from "@/api/hooks/useTenants";
import { useAuth } from "@/auth/useAuth";
import { env } from "@/lib/env";

export type ServingKpis = {
  match_count: number;
  local_only_count: number;
  national_only_count: number;
  conflict_count: number;
  reconciled_row_count: number;
  active_professional_count: number;
};

export type ServingOverview = {
  schema_version: string;
  tenant_id: string;
  run_id: string;
  generated_at: string;
  competencia: string;
  kpis: ServingKpis;
  divergence_counts: Record<string, number>;
  missing_sources: string[];
};

export type ServingEnvelope = {
  url: string;
  version_id: string;
  expires_in: number;
};

export type ServingDeliveryCode =
  | "serving_envelope_invalid"
  | "serving_url_invalid"
  | "serving_download_failed"
  | "serving_tenant_missing";

export class ServingDeliveryError extends Error {
  constructor(
    public readonly code: ServingDeliveryCode,
    public readonly reason: string,
  ) {
    super(`${code}=${reason}`);
    this.name = "ServingDeliveryError";
  }
}

const SERVING_OVERVIEW_PATH = "/dashboard/serving/cnes/overview";
const MAX_RETRIES = 1;
const ENVELOPE_KEYS = "expires_in,url,version_id";
// The build has no public bucket setting: pin the regional S3 host of the production region and
// a dot-free bucket label (a dotted bucket breaks the S3 wildcard certificate).
const SIGNED_HOST = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]\.s3\.us-east-2\.amazonaws\.com$/;
const SIGNED_PATH_PREFIX = "/serving/";

export function isServingUnavailable(error: unknown): boolean {
  if (!(error instanceof ApiError)) return false;
  const body = error.body as { detail?: string } | null;
  return error.status === 503 && body?.detail === "active_serving_unavailable";
}

export function shouldRetryServing(failureCount: number, error: unknown): boolean {
  return failureCount < MAX_RETRIES && !isServingUnavailable(error);
}

function isEnvelope(body: unknown): body is Record<string, unknown> {
  return typeof body === "object" && body !== null && !Array.isArray(body) && "url" in body;
}

function parseEnvelope(body: Record<string, unknown>): ServingEnvelope {
  if (Object.keys(body).sort().join() !== ENVELOPE_KEYS) {
    throw new ServingDeliveryError("serving_envelope_invalid", "keys");
  }
  const { url, version_id: versionId, expires_in: expiresIn } = body;
  if (typeof url !== "string" || typeof versionId !== "string" || versionId === "") {
    throw new ServingDeliveryError("serving_envelope_invalid", "fields");
  }
  if (typeof expiresIn !== "number" || !Number.isInteger(expiresIn) || expiresIn <= 0) {
    throw new ServingDeliveryError("serving_envelope_invalid", "expires_in");
  }
  return { url, version_id: versionId, expires_in: expiresIn };
}

function parseSignedUrl(raw: string): URL {
  try {
    return new URL(raw);
  } catch {
    throw new ServingDeliveryError("serving_url_invalid", "parse");
  }
}

function signedUrl(raw: string): URL {
  const url = parseSignedUrl(raw);
  if (url.protocol !== "https:") throw new ServingDeliveryError("serving_url_invalid", "protocol");
  if (url.username || url.password || !SIGNED_HOST.test(url.host)) {
    throw new ServingDeliveryError("serving_url_invalid", "host");
  }
  if (!url.pathname.startsWith(SIGNED_PATH_PREFIX)) {
    throw new ServingDeliveryError("serving_url_invalid", "path");
  }
  return url;
}

async function downloadSigned(url: URL): Promise<ServingOverview> {
  const res = await fetch(url.href, {
    method: "GET",
    credentials: "omit",
    redirect: "error",
    headers: {},
    referrerPolicy: "no-referrer",
  });
  if (!res.ok) throw new ServingDeliveryError("serving_download_failed", String(res.status));
  return (await res.json()) as ServingOverview;
}

export async function fetchServingOverview(
  tenantId: string | undefined,
  accessToken?: string | null,
): Promise<ServingOverview> {
  const body = await apiFetch<unknown>(SERVING_OVERVIEW_PATH, { tenantId, accessToken });
  if (!isEnvelope(body)) return body as ServingOverview;
  return downloadSigned(signedUrl(parseEnvelope(body).url));
}

type ServingOverviewResult = Pick<
  UseQueryResult<ServingOverview, unknown>,
  "data" | "error" | "isError" | "isLoading" | "refetch"
>;

function withoutTenant(
  tenants: UseQueryResult<AuthorizedMunicipality[]>,
  serving: UseQueryResult<ServingOverview, unknown>,
): ServingOverviewResult {
  // The serving refetch ignores `enabled` and would go out without X-Tenant-Id.
  const refetch: ServingOverviewResult["refetch"] = async (options) => {
    await tenants.refetch(options);
    return serving;
  };
  const base = { data: undefined, refetch };
  if (tenants.data !== undefined) {
    const error = new ServingDeliveryError("serving_tenant_missing", "empty");
    return { ...base, error, isError: true, isLoading: false };
  }
  if (tenants.isError) return { ...base, error: tenants.error, isError: true, isLoading: false };
  return { ...base, error: null, isError: false, isLoading: true };
}

export function useServingOverview(): ServingOverviewResult {
  const session = useAuth();
  const { tenants, selected } = useSelectedTenant();
  const tenantId = selected ? municipalityId(selected) : undefined;
  const ready =
    session.status === "authenticated" &&
    (env.VITE_AUTH_MODE === "local" || tenantId !== undefined);
  const serving = useQuery({
    queryKey: ["serving-overview", session.scope, tenantId ?? null],
    queryFn: () => fetchServingOverview(tenantId, session.accessToken),
    enabled: ready,
    staleTime: 30_000,
    refetchOnWindowFocus: false,
    retry: shouldRetryServing,
  });
  const { data, error, isError, isLoading, refetch } = serving;
  return ready ? { data, error, isError, isLoading, refetch } : withoutTenant(tenants, serving);
}
