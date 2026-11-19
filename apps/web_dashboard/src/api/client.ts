import { getAccessToken } from "@/auth/oidc";
import { env, stripTrailingSlash } from "@/lib/env";
import type { Env } from "@/lib/env";

export type ApiOptions = RequestInit & { tenantId?: string; accessToken?: string | null };

export type ApiClientConfig = Pick<Env, "VITE_API_BASE_URL" | "VITE_AUTH_MODE">;

export type ApiFetch = <T = unknown>(path: string, opts?: ApiOptions) => Promise<T>;

export type ApiBoundaryCode = "api_origin_mismatch" | "api_path_invalid" | "credentials_forbidden";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public body: unknown,
  ) {
    super(message);
  }
}

export class ApiBoundaryError extends Error {
  constructor(public readonly code: ApiBoundaryCode) {
    super(code);
    this.name = "ApiBoundaryError";
  }
}

const UNSAFE_BASE = /^\/\/|[?#\\]/;
const FOREIGN_ENDPOINT = /^(?:[a-z][a-z\d+.-]*:|[/\\]{2})/i;
const UNSAFE_ENDPOINT_CHARS = /[\\#\p{Cc}]/u;
const ENCODED_PATH_CHARS = /%(?:2e|2f|5c)/i;
const QUERY_STRING = /\?.*$/s;
const DOT_SEGMENTS = new Set([".", ".."]);
const WEB_PROTOCOLS = new Set(["http:", "https:"]);

function assertEndpointPath(path: string): void {
  if (FOREIGN_ENDPOINT.test(path)) throw new ApiBoundaryError("api_origin_mismatch");
  const pathname = path.replace(QUERY_STRING, "");
  if (
    UNSAFE_ENDPOINT_CHARS.test(path) ||
    ENCODED_PATH_CHARS.test(pathname) ||
    pathname.includes("//") ||
    pathname.split("/").some((segment) => DOT_SEGMENTS.has(segment))
  ) {
    throw new ApiBoundaryError("api_path_invalid");
  }
}

function parseApiUrl(input: string): URL {
  try {
    return new URL(input, window.location.origin);
  } catch {
    throw new ApiBoundaryError("api_origin_mismatch");
  }
}

function isWithinApi(url: URL, api: URL): boolean {
  const scope = api.pathname.endsWith("/") ? api.pathname : `${api.pathname}/`;
  return (
    WEB_PROTOCOLS.has(url.protocol) && url.origin === api.origin && url.pathname.startsWith(scope)
  );
}

function resolveEndpoint(rawBase: string, path: string): URL {
  if (UNSAFE_BASE.test(rawBase)) throw new ApiBoundaryError("api_origin_mismatch");
  assertEndpointPath(path);
  const base = stripTrailingSlash(rawBase);
  const api = parseApiUrl(base);
  const url = parseApiUrl(`${base}${path}`);
  if (!isWithinApi(url, api)) throw new ApiBoundaryError("api_origin_mismatch");
  return url;
}

function credentialsFor(
  mode: Env["VITE_AUTH_MODE"],
  requested: RequestCredentials | undefined,
): RequestCredentials {
  if (mode === "local") return requested ?? "include";
  if (requested === "include") throw new ApiBoundaryError("credentials_forbidden");
  return "omit";
}

function headersFor(opts: ApiOptions, token: string | null): Headers {
  const headers = new Headers(opts.headers);
  headers.delete("Authorization");
  headers.delete("X-Tenant-Id");
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (opts.tenantId) headers.set("X-Tenant-Id", opts.tenantId);
  if (opts.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  return headers;
}

async function readBody<T>(res: Response): Promise<T> {
  const text = await res.text();
  const body: unknown = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new ApiError(res.status, `http ${res.status}`, body);
  }
  return body as T;
}

export function createApiFetch(config: ApiClientConfig): ApiFetch {
  return async <T = unknown>(path: string, opts: ApiOptions = {}): Promise<T> => {
    const url = resolveEndpoint(config.VITE_API_BASE_URL, path);
    const credentials = credentialsFor(config.VITE_AUTH_MODE, opts.credentials);
    const token =
      config.VITE_AUTH_MODE === "oidc"
        ? opts.accessToken === undefined
          ? await getAccessToken()
          : opts.accessToken
        : null;
    const { tenantId: _tenantId, accessToken: _accessToken, ...init } = opts;
    const res = await fetch(url.href, { ...init, credentials, headers: headersFor(opts, token) });
    return readBody<T>(res);
  };
}

export const apiFetch: ApiFetch = createApiFetch(env);
