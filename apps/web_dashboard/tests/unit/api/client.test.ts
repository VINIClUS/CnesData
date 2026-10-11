import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { ApiError, apiFetch, createApiFetch } from "@/api/client";
import { getAccessToken } from "@/auth/oidc";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok-1"),
}));

const mockGetAccessToken = vi.mocked(getAccessToken);

describe("apiFetch", () => {
  test("anexa_authorization_bearer", async () => {
    let captured: string | null = null;
    server.use(
      http.get("/api/v1/dashboard/auth/me", ({ request }) => {
        captured = request.headers.get("authorization");
        return HttpResponse.json({});
      }),
    );
    await apiFetch("/dashboard/auth/me");
    expect(captured).toBe("Bearer tok-1");
  });

  test("anexa_x_tenant_id_quando_passado", async () => {
    let captured: string | null = null;
    server.use(
      http.get("/api/v1/dashboard/agents/status", ({ request }) => {
        captured = request.headers.get("x-tenant-id");
        return HttpResponse.json({ sources: [] });
      }),
    );
    await apiFetch("/dashboard/agents/status", { tenantId: "354130" });
    expect(captured).toBe("354130");
  });

  test("propaga_erro_http_como_ApiError", async () => {
    server.use(
      http.get("/api/v1/dashboard/auth/me", () =>
        HttpResponse.json({ detail: "x" }, { status: 500 }),
      ),
    );
    await expect(apiFetch("/dashboard/auth/me")).rejects.toBeInstanceOf(ApiError);
  });

  test("retorna_null_quando_resposta_vazia", async () => {
    server.use(
      http.get("/api/v1/dashboard/auth/me", () => new HttpResponse(null, { status: 200 })),
    );
    const r = await apiFetch("/dashboard/auth/me");
    expect(r).toBeNull();
  });

  test("seta_content_type_json_quando_body_presente", async () => {
    let captured: string | null = null;
    server.use(
      http.post("/api/v1/dashboard/jobs", ({ request }) => {
        captured = request.headers.get("content-type");
        return HttpResponse.json({ ok: true });
      }),
    );
    await apiFetch("/dashboard/jobs", { method: "POST", body: JSON.stringify({ x: 1 }) });
    expect(captured).toBe("application/json");
  });

  test("nao_anexa_authorization_quando_token_ausente", async () => {
    mockGetAccessToken.mockResolvedValueOnce(null);
    let captured: string | null = "header-was-not-captured";
    server.use(
      http.get("/api/v1/dashboard/auth/me", ({ request }) => {
        captured = request.headers.get("authorization");
        return HttpResponse.json({});
      }),
    );
    await apiFetch("/dashboard/auth/me");
    expect(captured).toBeNull();
  });

  test("modo_local_envia_credenciais_para_preservar_sessao_cookie", async () => {
    let credentials: RequestCredentials | null = null;
    let authorization: string | null = "header-was-not-captured";
    server.use(
      http.get("/api/v1/auth/me", ({ request }) => {
        credentials = request.credentials;
        authorization = request.headers.get("authorization");
        return HttpResponse.json({});
      }),
    );
    const localFetch = createApiFetch({ VITE_API_BASE_URL: "/api/v1", VITE_AUTH_MODE: "local" });

    await localFetch("/auth/me");

    expect(credentials).toBe("include");
    expect(authorization).toBeNull();
  });

  test("modo_local_respeita_credentials_explicito", async () => {
    let credentials: RequestCredentials | null = null;
    server.use(
      http.get("/api/v1/auth/me", ({ request }) => {
        credentials = request.credentials;
        return HttpResponse.json({});
      }),
    );
    const localFetch = createApiFetch({ VITE_API_BASE_URL: "/api/v1", VITE_AUTH_MODE: "local" });

    await localFetch("/auth/me", { credentials: "same-origin" });

    expect(credentials).toBe("same-origin");
  });

  test("modo_oidc_omite_credenciais", async () => {
    let credentials: RequestCredentials | null = null;
    server.use(
      http.get("/api/v1/dashboard/auth/me", ({ request }) => {
        credentials = request.credentials;
        return HttpResponse.json({});
      }),
    );

    await apiFetch("/dashboard/auth/me");

    expect(credentials).toBe("omit");
  });

  test("base_relativa_usa_a_origem_da_pagina", async () => {
    let url: string | null = null;
    server.use(
      http.get("/api/v1/dashboard/auth/me", ({ request }) => {
        url = request.url;
        return HttpResponse.json({});
      }),
    );

    await apiFetch("/dashboard/auth/me");

    expect(url).toBe(`${window.location.origin}/api/v1/dashboard/auth/me`);
  });
});
