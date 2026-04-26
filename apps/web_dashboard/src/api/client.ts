import { getAccessToken } from "@/auth/oidc";
import { env } from "@/lib/env";

export type ApiOptions = RequestInit & { tenantId?: string };

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
    public body: unknown,
  ) {
    super(message);
  }
}

export async function apiFetch<T = unknown>(path: string, opts: ApiOptions = {}): Promise<T> {
  const headers = new Headers(opts.headers);
  const token = await getAccessToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);
  if (opts.tenantId) headers.set("X-Tenant-Id", opts.tenantId);
  if (opts.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const url = `${env.VITE_API_BASE_URL}${path}`;
  const res = await fetch(url, { ...opts, headers });
  const text = await res.text();
  const body = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new ApiError(res.status, `http ${res.status}`, body);
  }
  return body as T;
}
