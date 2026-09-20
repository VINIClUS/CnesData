import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { isServingUnavailable, useServingOverview } from "@/api/hooks/useServingOverview";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const LEGACY_OVERVIEW_PATH = "/api/v1/dashboard/overview";
const LEGACY_FATURAMENTO_PATH = "/api/v1/dashboard/faturamento/by-establishment";

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
}

const FIXTURE = {
  schema_version: "cnes-serving-v1",
  tenant_id: "354130",
  run_id: "fixture-cnes-run-v1",
  generated_at: "2026-01-31T23:59:59Z",
  competencia: "2026-01",
  kpis: {
    match_count: 3,
    local_only_count: 2,
    national_only_count: 2,
    conflict_count: 1,
    reconciled_row_count: 7,
    active_professional_count: 6,
  },
  divergence_counts: { NOME_PROFISSIONAL: 1, CH_TOTAL: 1 },
  missing_sources: [] as string[],
};

describe("isServingUnavailable", () => {
  test("erro_que_nao_e_ApiError_nunca_e_indisponivel", () => {
    expect(isServingUnavailable(new TypeError("network"))).toBe(false);
    expect(isServingUnavailable(null)).toBe(false);
  });
});

describe("useServingOverview", () => {
  test("busca_apenas_serving_ativo_sem_header_de_tenant", async () => {
    let header: string | null = "header-was-not-captured";
    let capturedUrl = "";
    server.use(
      http.get(SERVING_PATH, ({ request }) => {
        header = request.headers.get("x-tenant-id");
        capturedUrl = request.url;
        return HttpResponse.json(FIXTURE);
      }),
    );

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.data).toBeDefined());

    expect(header).toBeNull();
    expect(capturedUrl).toContain(SERVING_PATH);
    expect(result.current.data?.kpis.match_count).toBe(3);
  });

  test("estado_503_indisponivel_nao_e_silencioso", async () => {
    server.use(
      http.get(SERVING_PATH, () =>
        HttpResponse.json({ detail: "active_serving_unavailable" }, { status: 503 }),
      ),
    );

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.data).toBeUndefined();
    expect(isServingUnavailable(result.current.error)).toBe(true);
  });

  test("erro_generico_nao_e_confundido_com_indisponivel", async () => {
    server.use(
      http.get(SERVING_PATH, () => HttpResponse.json({ detail: "boom" }, { status: 500 })),
    );

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(isServingUnavailable(result.current.error)).toBe(false);
  });

  test("payload_vazio_e_payload_com_fontes_ausentes", async () => {
    server.use(
      http.get(SERVING_PATH, () =>
        HttpResponse.json({ ...FIXTURE, divergence_counts: {}, missing_sources: [] }),
      ),
    );
    const empty = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(empty.result.current.data).toBeDefined());
    expect(empty.result.current.data?.missing_sources).toEqual([]);
    expect(empty.result.current.data?.divergence_counts).toEqual({});

    server.use(
      http.get(SERVING_PATH, () =>
        HttpResponse.json({ ...FIXTURE, missing_sources: ["CNES_NACIONAL"] }),
      ),
    );
    const partial = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(partial.result.current.data).toBeDefined());
    expect(partial.result.current.data?.missing_sources).toEqual(["CNES_NACIONAL"]);
  });

  test("nunca_chama_rotas_legadas_de_overview_ou_faturamento", async () => {
    const legacyOverview = vi.fn();
    const legacyFaturamento = vi.fn();
    server.use(
      http.get(SERVING_PATH, () => HttpResponse.json(FIXTURE)),
      http.get(LEGACY_OVERVIEW_PATH, () => {
        legacyOverview();
        return HttpResponse.json({});
      }),
      http.get(LEGACY_FATURAMENTO_PATH, () => {
        legacyFaturamento();
        return HttpResponse.json({});
      }),
    );

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.data).toBeDefined());

    expect(legacyOverview).not.toHaveBeenCalled();
    expect(legacyFaturamento).not.toHaveBeenCalled();
  });
});
