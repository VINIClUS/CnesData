import { QueryClient, QueryClientProvider, focusManager } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse, type JsonBodyType } from "msw";
import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";
import type { MockInstance } from "vitest";

import { server } from "../../../mocks/server";

import { ApiError } from "@/api/client";
import {
  type ServingDeliveryCode,
  type ServingOverview,
  ServingDeliveryError,
  fetchServingOverview,
  isServingUnavailable,
  shouldRetryServing,
  useServingOverview,
} from "@/api/hooks/useServingOverview";
import { env } from "@/lib/env";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const TENANTS_PATH = "/api/v1/dashboard/tenants";
const LEGACY_OVERVIEW_PATH = "/api/v1/dashboard/overview";
const LEGACY_FATURAMENTO_PATH = "/api/v1/dashboard/faturamento/by-establishment";
const TENANT = "354130";
const S3_ORIGIN = "https://cnesdata-prod-data.s3.us-east-2.amazonaws.com";
const S3_PATH = `/serving/${TENANT}/run-1/overview.json`;
const S3_URL = `${S3_ORIGIN}${S3_PATH}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=abc`;
const ENVELOPE = { url: S3_URL, version_id: "run-1", expires_in: 300 };
const S3_REQUEST_INIT = {
  method: "GET",
  credentials: "omit",
  redirect: "error",
  headers: {},
  referrerPolicy: "no-referrer",
};
const AUTH_MODE = env.VITE_AUTH_MODE;

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

let fetchSpy: MockInstance<typeof fetch>;

beforeEach(() => {
  fetchSpy = vi.spyOn(globalThis, "fetch");
  server.use(
    http.get(TENANTS_PATH, () =>
      HttpResponse.json([
        { ibge6: TENANT, ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" },
      ]),
    ),
  );
});

afterEach(() => {
  env.VITE_AUTH_MODE = AUTH_MODE;
  fetchSpy.mockRestore();
});

function serveEnvelope(body: JsonBodyType = ENVELOPE) {
  server.use(http.get(SERVING_PATH, () => HttpResponse.json(body)));
}

function serveS3(document: JsonBodyType = FIXTURE) {
  const requests: Request[] = [];
  server.use(
    http.get(`${S3_ORIGIN}${S3_PATH}`, ({ request }) => {
      requests.push(request);
      return HttpResponse.json(document);
    }),
  );
  return requests;
}

async function expectDeliveryError(
  call: Promise<unknown>,
  code: ServingDeliveryCode,
): Promise<ServingDeliveryError> {
  const error: unknown = await call.then(
    () => null,
    (reason: unknown) => reason,
  );
  expect(error).toBeInstanceOf(ServingDeliveryError);
  const delivery = error as ServingDeliveryError;
  expect(delivery.code).toBe(code);
  expect(delivery.message.startsWith(`${code}=`)).toBe(true);
  return delivery;
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

describe("fetchServingOverview", () => {
  test("segunda_chamada_ao_s3_sem_credenciais_nem_headers", async () => {
    let apiRequest: Request | undefined;
    server.use(
      http.get(SERVING_PATH, ({ request }) => {
        apiRequest = request;
        return HttpResponse.json(ENVELOPE);
      }),
    );
    const s3Requests = serveS3();

    const overview = await fetchServingOverview(TENANT);

    expect(overview).toEqual(FIXTURE);
    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(fetchSpy).toHaveBeenNthCalledWith(2, S3_URL, S3_REQUEST_INIT);
    expect(apiRequest?.headers.get("authorization")).toBe("Bearer tok");
    expect(apiRequest?.headers.get("x-tenant-id")).toBe(TENANT);
    expect(s3Requests).toHaveLength(1);
    expect(Array.from(s3Requests[0]?.headers.keys() ?? ["request-not-captured"])).toEqual([]);
    expect(s3Requests[0]?.credentials).toBe("omit");
  });

  test.each([
    ["http", S3_URL.replace("https://", "http://")],
    ["host_alheio", `https://evil.example${S3_PATH}`],
    ["host_global", `https://cnesdata-prod-data.s3.amazonaws.com${S3_PATH}`],
    ["outra_regiao", `https://cnesdata-prod-data.s3.us-east-1.amazonaws.com${S3_PATH}`],
    ["bucket_com_ponto", `https://cnesdata.prod.s3.us-east-2.amazonaws.com${S3_PATH}`],
    ["sufixo_alheio", `${S3_ORIGIN}.evil.example${S3_PATH}`],
    ["credencial_na_url", S3_URL.replace("https://", "https://user:pass@")],
    ["porta_explicita", S3_URL.replace(".com/", ".com:8443/")],
    ["prefixo_raw", `${S3_ORIGIN}/raw/${TENANT}/cnes/data.parquet`],
    ["travessia", `${S3_ORIGIN}/serving/../raw/${TENANT}/data.parquet`],
    ["relativa", S3_PATH],
    ["javascript", "javascript:alert(1)"],
  ])("rejeita_url_assinada_fora_do_padrao_%s", async (_case, url) => {
    serveEnvelope({ ...ENVELOPE, url });

    const error = await expectDeliveryError(fetchServingOverview(TENANT), "serving_url_invalid");

    expect(error.message).not.toContain(url);
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  test.each([
    ["chave_extra", { ...ENVELOPE, location: S3_URL }],
    ["sem_expires_in", { url: S3_URL, version_id: "run-1" }],
    ["url_nao_texto", { ...ENVELOPE, url: 42 }],
    ["versao_vazia", { ...ENVELOPE, version_id: "" }],
    ["expires_in_zero", { ...ENVELOPE, expires_in: 0 }],
    ["expires_in_fracionado", { ...ENVELOPE, expires_in: 1.5 }],
    ["expires_in_texto", { ...ENVELOPE, expires_in: "300" }],
  ])("rejeita_envelope_fora_do_contrato_%s", async (_case, body) => {
    serveEnvelope(body);

    await expectDeliveryError(fetchServingOverview(TENANT), "serving_envelope_invalid");

    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  test("falha_do_s3_vira_erro_de_download_sem_url", async () => {
    serveEnvelope();
    server.use(
      http.get(`${S3_ORIGIN}${S3_PATH}`, () =>
        HttpResponse.text("<Error>AccessDenied</Error>", { status: 403 }),
      ),
    );

    const error = await expectDeliveryError(
      fetchServingOverview(TENANT),
      "serving_download_failed",
    );

    expect(error.message).toBe("serving_download_failed=403");
  });

  test("redirect_do_s3_e_recusado", async () => {
    const followed = vi.fn();
    serveEnvelope();
    server.use(
      http.get(`${S3_ORIGIN}${S3_PATH}`, () =>
        HttpResponse.redirect("https://evil.example/copia.json", 307),
      ),
      http.get("https://evil.example/copia.json", () => {
        followed();
        return HttpResponse.json(FIXTURE);
      }),
    );

    await expect(fetchServingOverview(TENANT)).rejects.toThrow(TypeError);

    expect(followed).not.toHaveBeenCalled();
  });

  test("perfil_local_le_o_documento_direto_sem_tenant", async () => {
    let header: string | null = "header-was-not-captured";
    server.use(
      http.get(SERVING_PATH, ({ request }) => {
        header = request.headers.get("x-tenant-id");
        return HttpResponse.json(FIXTURE);
      }),
    );

    const overview = await fetchServingOverview(undefined);

    expect(overview).toEqual(FIXTURE);
    expect(header).toBeNull();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });
});

describe("useServingOverview", () => {
  test("primeira_chamada_leva_o_tenant_selecionado", async () => {
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

    expect(header).toBe(TENANT);
    expect(seenPaths).toEqual([TENANTS_PATH, SERVING_PATH]);
    expect(result.current.data?.kpis.match_count).toBe(3);
  });

  test("envelope_assinado_entrega_o_documento_do_s3", async () => {
    serveEnvelope();
    const s3Requests = serveS3(NEXT_FIXTURE);

    const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
    await waitFor(() => expect(result.current.data).toBeDefined());

    expect(result.current.data).toEqual(NEXT_FIXTURE);
    expect(s3Requests).toHaveLength(1);
  });

  test("perfil_local_busca_sem_esperar_tenant", async () => {
    env.VITE_AUTH_MODE = "local";
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
    expect(result.current.data).toEqual(FIXTURE);
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
