import { QueryClient, QueryClientProvider, focusManager } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { ApiError } from "@/api/client";
import {
  type ServingOverview,
  isServingUnavailable,
  shouldRetryServing,
  useServingOverview,
} from "@/api/hooks/useServingOverview";

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

const FIXTURE: ServingOverview = {
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
  missing_sources: [],
};

const NEXT_FIXTURE: ServingOverview = {
  schema_version: "cnes-serving-v1",
  tenant_id: "354130",
  run_id: "fixture-cnes-run-v2",
  generated_at: "2026-02-28T23:59:59Z",
  competencia: "2026-02",
  kpis: {
    match_count: 11,
    local_only_count: 0,
    national_only_count: 5,
    conflict_count: 4,
    reconciled_row_count: 20,
    active_professional_count: 9,
  },
  divergence_counts: { CNS: 4 },
  missing_sources: ["CNES_NACIONAL"],
};

function productionClient() {
  return new QueryClient({ defaultOptions: { queries: { staleTime: 15_000, retry: 1 } } });
}

function withClient(client: QueryClient) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  };
}

describe("isServingUnavailable", () => {
  test("erro_que_nao_e_ApiError_nunca_e_indisponivel", () => {
    expect(isServingUnavailable(new TypeError("network"))).toBe(false);
    expect(isServingUnavailable(null)).toBe(false);
  });
});

describe("shouldRetryServing", () => {
  const unavailable = new ApiError(503, "http 503", { detail: "active_serving_unavailable" });
  const serverError = new ApiError(500, "http 500", { detail: "boom" });

  test("serving_indisponivel_503_nunca_e_repetido", () => {
    expect(shouldRetryServing(0, unavailable)).toBe(false);
  });

  test("falha_generica_e_repetida_uma_vez_e_depois_desiste", () => {
    expect(shouldRetryServing(0, serverError)).toBe(true);
    expect(shouldRetryServing(1, serverError)).toBe(false);
  });

  test("falha_de_rede_e_repetida_uma_vez", () => {
    expect(shouldRetryServing(0, new TypeError("network"))).toBe(true);
    expect(shouldRetryServing(1, new TypeError("network"))).toBe(false);
  });
});

describe("useServingOverview", () => {
  test("busca_apenas_serving_ativo_sem_header_de_tenant", async () => {
    let header: string | null = "header-was-not-captured";
    const seenPaths: string[] = [];
    const record = ({ request }: { request: Request }) => {
      seenPaths.push(new URL(request.url, "http://localhost").pathname);
    };
    server.events.on("request:start", record);
    server.use(
      http.get(SERVING_PATH, ({ request }) => {
        header = request.headers.get("x-tenant-id");
        return HttpResponse.json(FIXTURE);
      }),
    );

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.data).toBeDefined());
    server.events.removeListener("request:start", record);

    expect(header).toBeNull();
    expect(seenPaths).toEqual([SERVING_PATH]);
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
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 4000 });

    expect(isServingUnavailable(result.current.error)).toBe(false);
  });

  test("serving_indisponivel_503_nao_e_repetido_pelo_hook", async () => {
    let requests = 0;
    server.use(
      http.get(SERVING_PATH, () => {
        requests += 1;
        return HttpResponse.json({ detail: "active_serving_unavailable" }, { status: 503 });
      }),
    );

    const { result } = renderHook(() => useServingOverview(), {
      wrapper: withClient(productionClient()),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 4000 });

    expect(requests).toBe(1);
  });

  test("falha_generica_e_repetida_no_maximo_uma_vez", async () => {
    let requests = 0;
    server.use(
      http.get(SERVING_PATH, () => {
        requests += 1;
        return HttpResponse.json({ detail: "boom" }, { status: 500 });
      }),
    );

    const { result } = renderHook(() => useServingOverview(), {
      wrapper: withClient(productionClient()),
    });
    await waitFor(() => expect(result.current.isError).toBe(true), { timeout: 4000 });

    expect(requests).toBe(2);
  });

  test("nao_refaz_a_busca_ao_focar_a_janela_com_dado_obsoleto", async () => {
    let requests = 0;
    server.use(
      http.get(SERVING_PATH, () => {
        requests += 1;
        return HttpResponse.json(FIXTURE);
      }),
    );
    const client = productionClient();
    const { result } = renderHook(() => useServingOverview(), { wrapper: withClient(client) });
    await waitFor(() => expect(result.current.data).toBeDefined());

    await client.invalidateQueries({ refetchType: "none" });
    act(() => {
      focusManager.setFocused(false);
      focusManager.setFocused(true);
    });
    await new Promise((resolve) => setTimeout(resolve, 100));

    expect(requests).toBe(1);
  });

  test("troca_de_pointer_substitui_o_payload_inteiro_sem_misturar_runs", async () => {
    let active: ServingOverview = FIXTURE;
    server.use(http.get(SERVING_PATH, () => HttpResponse.json(active)));
    const { result } = renderHook(() => useServingOverview(), {
      wrapper: withClient(productionClient()),
    });
    await waitFor(() => expect(result.current.data).toEqual(FIXTURE));

    active = NEXT_FIXTURE;
    await act(async () => {
      await result.current.refetch();
    });

    await waitFor(() => expect(result.current.data).toEqual(NEXT_FIXTURE));
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
