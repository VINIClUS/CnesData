import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { MockInstance } from "vitest";

import { server } from "../../mocks/server";

import { ApiBoundaryError, createApiFetch } from "@/api/client";
import type { ApiBoundaryCode } from "@/api/client";
import { getAccessToken } from "@/auth/oidc";
import { parseEnv } from "@/lib/env";

vi.mock("@/auth/oidc", () => ({ getAccessToken: vi.fn() }));

const API_BASE = "https://api.cnesdata.com.br/api/v1";
const ROOT_BASE = "https://api.example.test";

const mockGetAccessToken = vi.mocked(getAccessToken);
let fetchSpy: MockInstance<typeof fetch>;

beforeEach(() => {
  mockGetAccessToken.mockReset();
  mockGetAccessToken.mockResolvedValue("tok-prod");
  fetchSpy = vi.spyOn(globalThis, "fetch");
});

afterEach(() => {
  vi.restoreAllMocks();
});

function productionApiFetch(base = API_BASE) {
  return createApiFetch(
    parseEnv({
      VITE_RUNTIME_POLICY: "production",
      VITE_API_BASE_URL: base,
      VITE_AUTH_MODE: "oidc",
    }),
  );
}

function captureGets(url: string): Request[] {
  const requests: Request[] = [];
  server.use(
    http.get(url, ({ request }) => {
      requests.push(request);
      return HttpResponse.json({});
    }),
  );
  return requests;
}

async function expectBlocked(call: Promise<unknown>, code: ApiBoundaryCode): Promise<void> {
  await expect(call).rejects.toThrow(new ApiBoundaryError(code));
  expect(fetchSpy).not.toHaveBeenCalled();
  expect(mockGetAccessToken).not.toHaveBeenCalled();
}

const FOREIGN_ENDPOINTS = [
  "https://evil.test/x",
  "//evil.test/x",
  "/\\evil.test/x",
  "\\\\evil.test/x",
  `${API_BASE}/dashboard/auth/me`,
  "javascript:alert(1)",
  "dashboard/auth/me",
  "",
];

const MALFORMED_ENDPOINTS = [
  "/../admin",
  "/dashboard/../../admin",
  "/./dashboard/auth/me",
  "/dashboard/auth/me/.",
  "/%2e%2e/admin",
  "/.%2E/admin",
  "/..%2fadmin",
  "/..%5Cadmin",
  "/dashboard\\..\\admin",
  "/dashboard/auth/me#fragmento",
  "/dashboard//auth/me",
  "/dash\tboard",
];

describe("cliente da API em producao", () => {
  test("nao envia bearer fora da origem da API", async () => {
    await expectBlocked(productionApiFetch()("https://evil.test/x"), "api_origin_mismatch");
  });

  test("rejeita base relativa em producao", () => {
    expect(() => productionApiFetch("/api/v1")).toThrow(/VITE_API_BASE_URL/);
  });

  test("rejeita credentials include em producao", async () => {
    const call = productionApiFetch()("/dashboard/auth/me", { credentials: "include" });

    await expectBlocked(call, "credentials_forbidden");
  });

  test.each(FOREIGN_ENDPOINTS)(
    "rejeita endpoint absoluto e traversal: %j sai da API",
    async (path) => {
      await expectBlocked(productionApiFetch()(path), "api_origin_mismatch");
    },
  );

  test.each(MALFORMED_ENDPOINTS)(
    "rejeita endpoint absoluto e traversal: %j invalido",
    async (path) => {
      await expectBlocked(productionApiFetch()(path), "api_path_invalid");
    },
  );

  test("so envia X-Tenant-Id quando o tenant e explicito", async () => {
    const requests = captureGets(`${API_BASE}/dashboard/agents/status`);
    const apiFetch = productionApiFetch();

    await apiFetch("/dashboard/agents/status", { headers: { "X-Tenant-Id": "999999" } });
    await apiFetch("/dashboard/agents/status", {
      tenantId: "354130",
      headers: { "X-Tenant-Id": "999999" },
    });

    expect(requests.map((r) => r.headers.get("x-tenant-id"))).toEqual([null, "354130"]);
  });

  test("anexa bearer e omite credentials na origem da API", async () => {
    const requests = captureGets(`${API_BASE}/dashboard/auth/me`);

    await productionApiFetch()("/dashboard/auth/me");

    expect(fetchSpy).toHaveBeenCalledOnce();
    expect(requests).toHaveLength(1);
    expect(requests[0]?.url).toBe(`${API_BASE}/dashboard/auth/me`);
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer tok-prod");
    expect(requests[0]?.credentials).toBe("omit");
  });

  test("descarta authorization do chamador e usa so o token da sessao", async () => {
    const requests = captureGets(`${API_BASE}/dashboard/auth/me`);
    const apiFetch = productionApiFetch();
    const opts = { headers: { Authorization: "Bearer forjado" } };

    await apiFetch("/dashboard/auth/me", opts);
    mockGetAccessToken.mockResolvedValueOnce(null);
    await apiFetch("/dashboard/auth/me", opts);

    expect(requests.map((r) => r.headers.get("authorization"))).toEqual(["Bearer tok-prod", null]);
  });

  test("forca credentials omit mesmo com outro valor explicito", async () => {
    const requests = captureGets(`${API_BASE}/dashboard/auth/me`);

    await productionApiFetch()("/dashboard/auth/me", { credentials: "same-origin" });

    expect(requests[0]?.credentials).toBe("omit");
  });
});

describe("join estrito da URL da API", () => {
  test("normaliza uma barra final da base e preserva a query do endpoint", async () => {
    const requests = captureGets(`${API_BASE}/dashboard/agents/runs`);
    const apiFetch = productionApiFetch(`${API_BASE}/`);

    await apiFetch("/dashboard/agents/runs?limit=20&cursor=a%2Fb", { tenantId: "354130" });

    expect(requests[0]?.url).toBe(`${API_BASE}/dashboard/agents/runs?limit=20&cursor=a%2Fb`);
  });

  test.each([
    "/api/v1?x=1",
    "/api/v1#fragmento",
    "\\api\\v1",
    "//evil.test/api/v1",
    "https://exa mple.test/api/v1",
    "data:text/plain,api",
  ])("rejeita base %j antes de qualquer fetch", async (base) => {
    const apiFetch = createApiFetch({ VITE_API_BASE_URL: base, VITE_AUTH_MODE: "oidc" });

    await expectBlocked(apiFetch("/dashboard/auth/me"), "api_origin_mismatch");
  });

  test("monta a URL numa base sem caminho", async () => {
    const requests = captureGets(`${ROOT_BASE}/dashboard/auth/me`);
    const apiFetch = createApiFetch({ VITE_API_BASE_URL: `${ROOT_BASE}/`, VITE_AUTH_MODE: "oidc" });

    await apiFetch("/dashboard/auth/me");

    expect(requests[0]?.url).toBe(`${ROOT_BASE}/dashboard/auth/me`);
    expect(requests[0]?.headers.get("authorization")).toBe("Bearer tok-prod");
  });

  test.each([".evil.test/x", "@evil.test/x", ":8443/x", ":99999/x"])(
    "rejeita endpoint %j que altera a autoridade de uma base sem caminho",
    async (path) => {
      const apiFetch = createApiFetch({ VITE_API_BASE_URL: ROOT_BASE, VITE_AUTH_MODE: "oidc" });

      await expectBlocked(apiFetch(path), "api_origin_mismatch");
    },
  );
});
