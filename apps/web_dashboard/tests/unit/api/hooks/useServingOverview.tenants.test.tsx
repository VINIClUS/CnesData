import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse, type JsonBodyType } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { ApiError } from "@/api/client";
import {
  type ServingOverview,
  ServingDeliveryError,
  useServingOverview,
} from "@/api/hooks/useServingOverview";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const TENANTS_PATH = "/api/v1/dashboard/tenants";
const TENANT = "354130";
const OTHER_TENANT = "350950";
const TENANT_ROW = { ibge6: TENANT, ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" };

function wrap({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
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

function productionClient() {
  return new QueryClient({ defaultOptions: { queries: { staleTime: 15_000, retry: 1 } } });
}

function withClient(client: QueryClient) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>;
  };
}

function serveOverview(body: JsonBodyType = FIXTURE, init?: ResponseInit) {
  const tenants: (string | null)[] = [];
  server.use(
    http.get(SERVING_PATH, ({ request }) => {
      tenants.push(request.headers.get("x-tenant-id"));
      return HttpResponse.json(body, init);
    }),
  );
  return tenants;
}

describe("useServingOverview", () => {
  test("tenants_pendentes_mostram_carregando", async () => {
    let listTenants: (response: Response) => void = () => undefined;
    const listed = new Promise<Response>((resolve) => {
      listTenants = resolve;
    });
    server.use(http.get(TENANTS_PATH, () => listed));
    const tenants = serveOverview();

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(result.current).toMatchObject({ isLoading: true, isError: false, data: undefined });
    expect(tenants).toEqual([]);

    listTenants(HttpResponse.json([TENANT_ROW]));
    await waitFor(() => expect(result.current.data).toEqual(FIXTURE));
    expect(tenants).toEqual([TENANT]);
  });

  test.each([
    [400, "tenant_header_required"],
    [500, "boom"],
  ])("falha_ao_listar_tenants_vira_erro_visivel_%i", async (status, detail) => {
    server.use(http.get(TENANTS_PATH, () => HttpResponse.json({ detail }, { status })));
    const tenants = serveOverview();

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.error).toBeInstanceOf(ApiError);
    expect(result.current.error).toMatchObject({ status, body: { detail } });
    expect(result.current).toMatchObject({ isLoading: false, data: undefined });
    expect(tenants).toEqual([]);
  });

  test("lista_de_tenants_vazia_vira_erro_visivel", async () => {
    server.use(http.get(TENANTS_PATH, () => HttpResponse.json([])));
    const tenants = serveOverview();

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.isError).toBe(true));

    expect(result.current.error).toBeInstanceOf(ServingDeliveryError);
    expect(result.current.error).toMatchObject({ message: "serving_tenant_missing=empty" });
    expect(result.current).toMatchObject({ isLoading: false, data: undefined });
    expect(tenants).toEqual([]);
  });

  test("troca_de_tenant_busca_de_novo_sem_reusar_cache_do_outro_tenant", async () => {
    let selected = TENANT;
    const seen: (string | null)[] = [];
    server.use(
      http.get(TENANTS_PATH, () => HttpResponse.json([{ ...TENANT_ROW, ibge6: selected }])),
      http.get(SERVING_PATH, ({ request }) => {
        const tenant = request.headers.get("x-tenant-id");
        seen.push(tenant);
        return HttpResponse.json({ ...FIXTURE, tenant_id: tenant });
      }),
    );
    const client = productionClient();
    const { result } = renderHook(() => useServingOverview(), { wrapper: withClient(client) });
    await waitFor(() => expect(result.current.data?.tenant_id).toBe(TENANT));

    selected = OTHER_TENANT;
    await act(() => client.invalidateQueries({ queryKey: ["tenants"] }));

    await waitFor(() => expect(result.current.data?.tenant_id).toBe(OTHER_TENANT));
    expect(seen).toEqual([TENANT, OTHER_TENANT]);
  });

  test("falha_so_na_revalidacao_dos_tenants_mantem_o_overview", async () => {
    let failing = false;
    server.use(
      http.get(TENANTS_PATH, () =>
        failing
          ? HttpResponse.json({ detail: "boom" }, { status: 500 })
          : HttpResponse.json([TENANT_ROW]),
      ),
    );
    serveOverview();
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { result } = renderHook(() => useServingOverview(), { wrapper: withClient(client) });
    await waitFor(() => expect(result.current.data).toEqual(FIXTURE));

    failing = true;
    await act(() => client.invalidateQueries({ queryKey: ["tenants"] }));
    await waitFor(() => expect(client.getQueryState(["tenants"])?.status).toBe("error"));

    expect(result.current).toMatchObject({ data: FIXTURE, isError: false, error: null });
  });

  test("refetch_sem_tenant_nao_chama_o_serving", async () => {
    let tenantCalls = 0;
    server.use(
      http.get(TENANTS_PATH, () => {
        tenantCalls += 1;
        return HttpResponse.json([]);
      }),
    );
    const requests = serveOverview();
    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.isError).toBe(true));
    expect(tenantCalls).toBe(1);

    await act(async () => {
      await result.current.refetch();
    });

    expect(requests).toEqual([]);
    expect(tenantCalls).toBe(2);
  });
});
