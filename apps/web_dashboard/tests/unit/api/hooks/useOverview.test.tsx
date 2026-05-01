import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { useOverview } from "@/api/hooks/useOverview";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

describe("useOverview", () => {
  test("fetcha_kpis", async () => {
    server.use(
      http.get("/api/v1/dashboard/overview", () =>
        HttpResponse.json({
          competencia_atual: 202604,
          faturamento_atual_cents: 120_000_000,
          faturamento_anterior_cents: 115_000_000,
          aih_atual: 312,
          aih_anterior: 340,
          profissionais_ativos: 421,
          profissionais_anterior: 419,
          estabs_sem_producao: 7,
          estabs_total: 124,
          estabs_sem_producao_anterior: 5,
        }),
      ),
    );
    const { result } = renderHook(() => useOverview("354130"), { wrapper: wrap });
    await waitFor(() => expect(result.current.data).toBeDefined());
    expect(result.current.data?.faturamento_atual_cents).toBe(120_000_000);
    expect(result.current.data?.estabs_sem_producao).toBe(7);
  });
});
