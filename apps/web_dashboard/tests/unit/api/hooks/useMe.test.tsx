import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { useMe } from "@/api/hooks/useMe";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

describe("useMe", () => {
  test("fetcha_perfil_do_endpoint", async () => {
    server.use(
      http.get("/api/v1/dashboard/auth/me", () =>
        HttpResponse.json({
          user_id: "u-1",
          email: "g@m",
          display_name: null,
          role: "gestor",
          tenant_ids: ["354130"],
        }),
      ),
    );
    const { result } = renderHook(() => useMe(), { wrapper: wrap });
    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data?.email).toBe("g@m");
  });
});
