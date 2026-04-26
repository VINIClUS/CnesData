import { useAgentStatus } from "@/api/hooks/useAgentStatus";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";
import { server } from "../../../mocks/server";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

describe("useAgentStatus", () => {
  test("fetcha_e_anexa_x_tenant_id", async () => {
    let header: string | null = null;
    server.use(
      http.get("/api/v1/dashboard/agents/status", ({ request }) => {
        header = request.headers.get("x-tenant-id");
        return HttpResponse.json({
          fetched_at: "2026-04-25T12:00:00Z",
          sources: [
            {
              fonte_sistema: "CNES_LOCAL",
              last_extracao_ts: "2026-04-25T06:00:00Z",
              last_competencia: 202604,
              lag_months: 0,
              row_count: 4521,
              status: "ok",
              last_machine_id: null,
            },
          ],
        });
      }),
    );
    const { result } = renderHook(() => useAgentStatus("354130"), {
      wrapper: wrap,
    });
    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(header).toBe("354130");
    expect(result.current.data?.sources[0]?.fonte_sistema).toBe("CNES_LOCAL");
  });
});
