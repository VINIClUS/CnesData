import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { useMe } from "@/api/hooks/useMe";
import { useServingOverview } from "@/api/hooks/useServingOverview";
import { useTenants } from "@/api/hooks/useTenants";
import { AuthProvider } from "@/auth/AuthProvider";
import { getAccessToken } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";
import { env } from "@/lib/env";

vi.mock("@/auth/oidc", () => ({ getAccessToken: vi.fn() }));
let token: string | null = "A";
const ME_PATH = "/api/v1/dashboard/auth/me";
const grants = (id: string) => [
  { tenant_id: `tenant-${id}`, municipality_name: `Município ${id}`, role: "gestor" },
];
const legacy = (id: string) => ({
  user_id: id,
  email: `${id}@example.test`,
  display_name: id,
  role: "gestor",
  tenant_ids: [`tenant-${id}`],
  has_pending_request: false,
});
const identity = (id: string, subject = id) => ({
  subject,
  issuer: `https://${id}.idp.example.test`,
  email: `${id}@example.test`,
  display_name: id,
});
const principal = (request: Request) =>
  request.headers.get("authorization")?.slice(7) ?? "anonymous";

beforeEach(() => {
  token = "A";
  env.VITE_AUTH_MODE = "oidc";
  vi.mocked(getAccessToken).mockImplementation(() => Promise.resolve(token));
});

function Probe() {
  const { user, status, refresh } = useAuth();
  const me = useMe();
  const tenants = useTenants();
  const serving = useServingOverview();
  return (
    <>
      <output data-testid="session">
        {status}|{user?.email}
      </output>
      <output data-testid="me">{JSON.stringify(me.data)}</output>
      <output data-testid="tenants">{JSON.stringify(tenants.data)}</output>
      <output data-testid="serving">{JSON.stringify(serving.data)}</output>
      <button onClick={() => void refresh()}>Atualizar sessão</button>
    </>
  );
}

function mount() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={client}>
      <AuthProvider>
        <Probe />
      </AuthProvider>
    </QueryClientProvider>,
  );
  return client;
}

function network(aws = false, sameSubject = false) {
  const seen: { user: string; tenant: string | null }[] = [];
  server.use(
    http.get(ME_PATH, ({ request }) =>
      HttpResponse.json(
        aws
          ? identity(principal(request), sameSubject ? "shared-subject" : principal(request))
          : legacy(principal(request)),
      ),
    ),
    http.get("/api/v1/dashboard/memberships", ({ request }) =>
      HttpResponse.json(grants(principal(request))),
    ),
    http.get("/api/v1/dashboard/tenants", ({ request }) =>
      HttpResponse.json([
        {
          ibge6: `tenant-${principal(request)}`,
          ibge7: "3541308",
          nome: `Município ${principal(request)}`,
          uf: "SP",
        },
      ]),
    ),
    http.get("/api/v1/dashboard/serving/cnes/overview", ({ request }) => {
      const tenant = request.headers.get("x-tenant-id");
      seen.push({ user: principal(request), tenant });
      return HttpResponse.json({
        schema_version: "cnes-serving-v1",
        tenant_id: tenant,
        run_id: principal(request),
        generated_at: "2026-10-25T00:00:00Z",
        competencia: "2026-10",
        kpis: {
          match_count: 1,
          local_only_count: 0,
          national_only_count: 0,
          conflict_count: 0,
          reconciled_row_count: 1,
          active_professional_count: 1,
        },
        divergence_counts: {},
        missing_sources: [],
      });
    }),
  );
  return seen;
}

function awsGrants(id: string, sharedTenant: boolean) {
  return [
    {
      tenant_id: sharedTenant ? "tenant-shared" : `tenant-${id}`,
      municipality_name: sharedTenant ? "Município compartilhado" : `Município ${id}`,
      role: id === "A" ? "gestor" : "leitor",
    },
  ];
}

function awsOverview(id: string, tenant: string | null) {
  return {
    schema_version: "cnes-serving-v1",
    tenant_id: tenant,
    run_id: id,
    generated_at: "2026-10-25T00:00:00Z",
    competencia: "2026-10",
    kpis: {
      match_count: 1,
      local_only_count: 0,
      national_only_count: 0,
      conflict_count: 0,
      reconciled_row_count: 1,
      active_professional_count: 1,
    },
    divergence_counts: {},
    missing_sources: [],
  };
}

function awsNetwork(sharedTenant = false, pendingA?: Promise<Response>) {
  const serving = network(true, true);
  const memberships: { bearer: string | null; tenant: string | null }[] = [];
  const product: { bearer: string | null; tenant: string | null }[] = [];
  const completedMemberships: string[] = [];
  server.use(
    http.get("/api/v1/dashboard/tenants", () =>
      HttpResponse.json({ detail: "tenant_header_required" }, { status: 400 }),
    ),
    http.get("/api/v1/dashboard/memberships", async ({ request }) => {
      const id = principal(request);
      memberships.push({
        bearer: request.headers.get("authorization"),
        tenant: request.headers.get("x-tenant-id"),
      });
      const response =
        id === "A" && pendingA ? await pendingA : HttpResponse.json(awsGrants(id, sharedTenant));
      completedMemberships.push(id);
      return response;
    }),
  );
  server.use(
    http.get("/api/v1/dashboard/serving/cnes/overview", ({ request }) => {
      const id = principal(request);
      const tenant = request.headers.get("x-tenant-id");
      serving.push({ user: id, tenant });
      product.push({ bearer: request.headers.get("authorization"), tenant });
      return HttpResponse.json(awsOverview(id, tenant));
    }),
  );
  return { serving, memberships, product, completedMemberships };
}

function deferredResponse() {
  let release: (response: Response) => void = () => undefined;
  const pending = new Promise<Response>((resolve) => {
    release = resolve;
  });
  return { pending, release };
}

async function changeSession(next: string | null) {
  token = next;
  await userEvent.click(screen.getByRole("button", { name: "Atualizar sessão" }));
  await waitFor(() =>
    expect(screen.getByTestId("session")).toHaveTextContent(
      next ? `authenticated|${next}@example.test` : "anonymous|",
    ),
  );
}

describe("isolamento de identidade e caches", () => {
  test.each([false, true])(
    "cache_me_isolado_por_issuer_e_subject_mesmo_subject_%s",
    async (sameSubject) => {
      network(true, sameSubject);
      mount();
      await waitFor(() =>
        expect(screen.getByTestId("me")).toHaveTextContent("https://A.idp.example.test"),
      );
      await changeSession("B");
      await waitFor(() =>
        expect(screen.getByTestId("me")).toHaveTextContent("https://B.idp.example.test"),
      );
      expect(screen.getByTestId("me")).not.toHaveTextContent("https://A.idp.example.test");
    },
  );

  test("troca_de_usuario_nao_reutiliza_memberships_tenant_selecionado_ou_serving", async () => {
    const seen = network();
    mount();
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("tenant-A"));
    await changeSession("B");
    expect(screen.getByTestId("tenants")).not.toHaveTextContent("Município A");
    expect(screen.getByTestId("serving")).not.toHaveTextContent("tenant-A");
    await waitFor(() => expect(seen).toContainEqual({ user: "B", tenant: "tenant-B" }));
    expect(seen).not.toContainEqual({ user: "B", tenant: "tenant-A" });
  });

  test("logout_e_login_nao_reabrem_dados_do_usuario_anterior", async () => {
    network();
    mount();
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("tenant-A"));
    await changeSession(null);
    expect(screen.getByTestId("tenants")).not.toHaveTextContent("Município A");
    expect(screen.getByTestId("serving")).not.toHaveTextContent("tenant-A");
    await changeSession("B");
    await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent("Município B"));
  });

  test("resposta_atrasada_de_A_nao_sobrescreve_sessao_e_cache_de_B", async () => {
    network();
    let release: (response: Response) => void = () => undefined;
    let blocked = false;
    const response = new Promise<Response>((resolve) => {
      release = resolve;
    });
    server.use(
      http.get(ME_PATH, ({ request }) => {
        if (principal(request) === "A") {
          blocked = true;
          return response;
        }
        return HttpResponse.json(legacy("B"));
      }),
    );
    mount();
    await waitFor(() => expect(blocked).toBe(true));
    await changeSession("B");
    await act(async () => {
      release(HttpResponse.json(legacy("A")));
      await response;
    });
    await waitFor(() => expect(screen.getByTestId("me")).not.toHaveTextContent("A@example.test"));
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated|B@example.test");
    expect(screen.getByTestId("tenants")).not.toHaveTextContent("Município A");
  });
  test("sessao_legada_unica_exibe_somente_tenant_e_serving_autorizados", async () => {
    const seen = network();
    mount();
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent("tenant-A"));
    expect(screen.getByTestId("session")).toHaveTextContent("authenticated|A@example.test");
    expect(screen.getByTestId("tenants")).toHaveTextContent("Município A");
    expect(seen).toEqual([{ user: "A", tenant: "tenant-A" }]);
  });

  test("memberships_aws_nao_vazam_depois_da_troca_de_principal", async () => {
    network(true);
    server.use(
      http.get("/api/v1/dashboard/tenants", () =>
        HttpResponse.json({ detail: "tenant_header_required" }, { status: 400 }),
      ),
    );
    mount();
    await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent("Município A"));
    await changeSession("B");
    expect(screen.getByTestId("tenants")).not.toHaveTextContent("Município A");
    await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent("Município B"));
    expect(screen.getByTestId("tenants")).not.toHaveTextContent("ibge7");
  });

  test("resposta_atrasada_de_tenants_de_A_nao_reaparece_para_B", async () => {
    network();
    let release: (response: Response) => void = () => undefined;
    let blocked = false;
    const pending = new Promise<Response>((resolve) => {
      release = resolve;
    });
    server.use(
      http.get("/api/v1/dashboard/tenants", ({ request }) => {
        if (principal(request) === "A") {
          blocked = true;
          return pending;
        }
        return HttpResponse.json([
          { ibge6: "tenant-B", ibge7: "3509502", nome: "Município B", uf: "SP" },
        ]);
      }),
    );
    mount();
    await waitFor(() => expect(blocked).toBe(true));
    await changeSession("B");
    await act(async () => {
      release(
        HttpResponse.json([{ ibge6: "tenant-A", ibge7: "3541308", nome: "Município A", uf: "SP" }]),
      );
      await pending;
    });
    await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent("Município B"));
    expect(screen.getByTestId("tenants")).not.toHaveTextContent("Município A");
    expect(screen.getByTestId("serving")).not.toHaveTextContent("tenant-A");
  });
  test("aws_mesmo_subject_outro_issuer_isola_papel_e_serving_do_tenant_compartilhado", async () => {
    const seen = awsNetwork(true);
    mount();
    await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent('"role":"gestor"'));
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"A"'));
    expect(screen.getByTestId("serving")).toHaveTextContent('"tenant_id":"tenant-shared"');
    expect(seen.memberships).toContainEqual({ bearer: "Bearer A", tenant: null });
    expect(seen.serving).toContainEqual({ user: "A", tenant: "tenant-shared" });

    await changeSession("B");

    expect(screen.getByTestId("tenants")).not.toHaveTextContent('"role":"gestor"');
    expect(screen.getByTestId("serving")).not.toHaveTextContent('"run_id":"A"');
    await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent('"role":"leitor"'));
    await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"B"'));
    expect(screen.getByTestId("serving")).toHaveTextContent('"tenant_id":"tenant-shared"');
    expect(screen.getByTestId("me")).toHaveTextContent('"subject":"shared-subject"');
    await waitFor(() =>
      expect(screen.getByTestId("me")).toHaveTextContent("https://B.idp.example.test"),
    );
    expect(screen.getByTestId("me")).not.toHaveTextContent("https://A.idp.example.test");
    expect(seen.memberships).toContainEqual({ bearer: "Bearer B", tenant: null });
    expect(seen.serving).toContainEqual({ user: "B", tenant: "tenant-shared" });
    expect(seen.product).toContainEqual({ bearer: "Bearer B", tenant: "tenant-shared" });
    expect(seen.memberships.every((request) => request.tenant === null)).toBe(true);
  });

  test("memberships_aws_atrasadas_de_A_nao_retrocedem_grants_e_tenant_ja_carregados_de_B", async () => {
    const deferred = deferredResponse();
    const seen = awsNetwork(false, deferred.pending);
    const client = mount();
    try {
      await waitFor(() =>
        expect(seen.memberships).toContainEqual({ bearer: "Bearer A", tenant: null }),
      );
      await changeSession("B");
      await waitFor(() => expect(screen.getByTestId("tenants")).toHaveTextContent("Município B"));
      await waitFor(() => expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"B"'));
      expect(screen.getByTestId("tenants")).toHaveTextContent('"role":"leitor"');
      expect(seen.memberships).toContainEqual({ bearer: "Bearer B", tenant: null });
      expect(seen.serving).toContainEqual({ user: "B", tenant: "tenant-B" });
      expect(seen.product).toContainEqual({ bearer: "Bearer B", tenant: "tenant-B" });

      expect(seen.completedMemberships).not.toContain("A");
      await act(async () => {
        deferred.release(HttpResponse.json(awsGrants("A", false)));
        await deferred.pending;
      });
      await waitFor(() => expect(client.isFetching()).toBe(0));
      expect(seen.completedMemberships).toContain("A");

      expect(screen.getByTestId("session")).toHaveTextContent("authenticated|B@example.test");
      expect(screen.getByTestId("tenants")).toHaveTextContent("Município B");
      expect(screen.getByTestId("tenants")).toHaveTextContent('"role":"leitor"');
      expect(screen.getByTestId("tenants")).not.toHaveTextContent("Município A");
      expect(screen.getByTestId("tenants")).not.toHaveTextContent('"role":"gestor"');
      expect(screen.getByTestId("serving")).toHaveTextContent('"tenant_id":"tenant-B"');
      expect(screen.getByTestId("serving")).toHaveTextContent('"run_id":"B"');
      expect(screen.getByTestId("serving")).not.toHaveTextContent("tenant-A");
      expect(seen.serving).not.toContainEqual({ user: "B", tenant: "tenant-A" });
      expect(seen.product).not.toContainEqual({ bearer: "Bearer B", tenant: "tenant-A" });
    } finally {
      deferred.release(HttpResponse.json(awsGrants("A", false)));
    }
  });
});
