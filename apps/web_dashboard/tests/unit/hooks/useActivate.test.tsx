import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import type * as ClientModule from "@/api/client";
import { ApiBoundaryError, apiFetch } from "@/api/client";
import { useActivate } from "@/api/hooks/useActivate";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

vi.mock("@/api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof ClientModule>();
  return { ...actual, apiFetch: vi.fn(actual.apiFetch) };
});

const ACTIVATE_URL = "/api/v1/activate/confirm";
const LEGACY_URL = "/activate/confirm";
const BODY = { user_code: "ABCD-EFGH", tenant_id: "354130" };
const APPROVED = { status: "approved", expires_in_seconds: 300 };
const mockApiFetch = vi.mocked(apiFetch);

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

function renderActivate() {
  return renderHook(() => useActivate(), { wrapper: wrap }).result;
}

beforeEach(() => {
  mockApiFetch.mockReset();
});

afterEach(() => {
  vi.restoreAllMocks();
});

describe("useActivate", () => {
  test("ativacao usa o cliente versionado e nao chama fetch global", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    mockApiFetch.mockResolvedValueOnce(APPROVED);
    const result = renderActivate();

    let data: unknown = null;
    await act(async () => {
      data = await result.current.mutateAsync(BODY);
    });

    expect(data).toEqual(APPROVED);
    expect(mockApiFetch).toHaveBeenCalledOnce();
    expect(mockApiFetch).toHaveBeenCalledWith("/activate/confirm", {
      method: "POST",
      tenantId: BODY.tenant_id,
      body: JSON.stringify(BODY),
    });
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("submete_user_code_e_tenant", async () => {
    const legacy: string[] = [];
    const seen: { body: unknown; authorization: string | null; tenant: string | null }[] = [];
    server.use(
      http.post(LEGACY_URL, ({ request }) => {
        legacy.push(request.url);
        return HttpResponse.json(APPROVED);
      }),
      http.post(ACTIVATE_URL, async ({ request }) => {
        seen.push({
          body: await request.json(),
          authorization: request.headers.get("authorization"),
          tenant: request.headers.get("x-tenant-id"),
        });
        return HttpResponse.json(APPROVED);
      }),
    );
    const result = renderActivate();

    await act(async () => {
      await result.current.mutateAsync(BODY);
    });

    expect(seen).toEqual([{ body: BODY, authorization: "Bearer tok", tenant: BODY.tenant_id }]);
    expect(legacy).toEqual([]);
  });

  test("propaga_erro_400_invalid_user_code", async () => {
    server.use(
      http.post(ACTIVATE_URL, () =>
        HttpResponse.json({ detail: "invalid_or_expired_user_code" }, { status: 400 }),
      ),
    );
    const result = renderActivate();

    await expect(
      result.current.mutateAsync({ user_code: "EXPIRED1", tenant_id: "354130" }),
    ).rejects.toThrow(new Error("invalid_or_expired_user_code"));
  });

  test.each([
    ["sem corpo", () => new HttpResponse(null, { status: 503 })],
    ["com detail nao textual", () => HttpResponse.json({ detail: [{}] }, { status: 422 })],
  ])("erro http %s vira activate_failed", async (_caso, respond: () => Response) => {
    server.use(http.post(ACTIVATE_URL, respond));
    const result = renderActivate();

    await expect(result.current.mutateAsync(BODY)).rejects.toThrow(new Error("activate_failed"));
  });

  test.each([
    [
      "500 texto puro",
      () =>
        new HttpResponse("Internal Server Error", {
          status: 500,
          headers: { "Content-Type": "text/plain" },
        }),
    ],
    [
      "502 html",
      () =>
        new HttpResponse("<html><body>Bad Gateway</body></html>", {
          status: 502,
          headers: { "Content-Type": "text/html" },
        }),
    ],
  ])("erro http com corpo nao json (%s) vira activate_failed", async (_caso, respond) => {
    server.use(http.post(ACTIVATE_URL, respond));
    const result = renderActivate();

    await expect(result.current.mutateAsync(BODY)).rejects.toThrow(new Error("activate_failed"));
  });

  test.each([
    ["sem corpo", () => new HttpResponse(null, { status: 200 })],
    ["sem status approved", () => HttpResponse.json({ status: "pending" })],
  ])("resposta 2xx %s nao aprova", async (_caso, respond: () => Response) => {
    server.use(http.post(ACTIVATE_URL, respond));
    const result = renderActivate();

    await expect(result.current.mutateAsync(BODY)).rejects.toThrow(new Error("activate_failed"));
  });

  test("falha de rede propaga sem traducao", async () => {
    server.use(http.post(ACTIVATE_URL, () => HttpResponse.error()));
    const result = renderActivate();

    const error = await result.current.mutateAsync(BODY).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(TypeError);
    expect((error as Error).message).not.toBe("activate_failed");
  });

  test("erro de fronteira do cliente propaga sem traducao", async () => {
    const boundary = new ApiBoundaryError("api_origin_mismatch");
    mockApiFetch.mockRejectedValueOnce(boundary);
    const result = renderActivate();

    await expect(result.current.mutateAsync(BODY)).rejects.toBe(boundary);
  });
});
