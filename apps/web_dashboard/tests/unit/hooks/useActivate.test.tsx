import { useActivate } from "@/api/hooks/useActivate";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";
import { server } from "../../mocks/server";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

describe("useActivate", () => {
  test("submete_user_code_e_tenant", async () => {
    let captured: unknown = null;
    server.use(
      http.post("/activate/confirm", async ({ request }) => {
        captured = await request.json();
        return HttpResponse.json({ status: "approved", expires_in_seconds: 300 });
      }),
    );
    const { result } = renderHook(() => useActivate(), { wrapper: wrap });
    await act(async () => {
      await result.current.mutateAsync({ user_code: "ABCD-EFGH", tenant_id: "354130" });
    });
    expect(captured).toEqual({ user_code: "ABCD-EFGH", tenant_id: "354130" });
  });

  test("propaga_erro_400_invalid_user_code", async () => {
    server.use(
      http.post("/activate/confirm", () =>
        HttpResponse.json({ detail: "invalid_or_expired_user_code" }, { status: 400 }),
      ),
    );
    const { result } = renderHook(() => useActivate(), { wrapper: wrap });
    await expect(
      result.current.mutateAsync({ user_code: "EXPIRED1", tenant_id: "354130" }),
    ).rejects.toThrow("invalid_or_expired_user_code");
  });
});
