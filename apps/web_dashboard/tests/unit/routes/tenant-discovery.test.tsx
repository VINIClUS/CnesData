import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRouter,
} from "@tanstack/react-router";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { useServingOverview } from "@/api/hooks/useServingOverview";
import { AuthProvider } from "@/auth/AuthProvider";
import { Shell } from "@/components/layout/Shell";
import { env } from "@/lib/env";
import { Route } from "@/routes/_app";
import { ThemeProvider } from "@/theme/ThemeProvider";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("test-token"),
  logout: vi.fn(),
}));

const ME = {
  subject: "cognito-nao-uuid",
  issuer: "https://idp.example.test",
  email: null,
  display_name: null,
};
const MEMBERSHIPS = [
  { tenant_id: "tenant-a", municipality_name: "Presidente Epitácio", role: "gestor" },
  { tenant_id: "tenant-z", municipality_name: "Campinas", role: "leitor" },
];
const ME_PATH = "/api/v1/dashboard/auth/me";
const MEMBERSHIPS_PATH = "/api/v1/dashboard/memberships";
const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const guard = Route.options.beforeLoad as () => Promise<unknown>;

beforeEach(() => {
  env.VITE_AUTH_MODE = "oidc";
  env.VITE_RUNTIME_POLICY = "generic";
});

function discovery(grants = MEMBERSHIPS, status = 200) {
  const requests: { url: string; bearer: string | null; tenant: string | null }[] = [];
  const capture = (request: Request) =>
    requests.push({
      url: new URL(request.url).pathname,
      bearer: request.headers.get("authorization"),
      tenant: request.headers.get("x-tenant-id"),
    });
  server.use(
    http.get(ME_PATH, ({ request }) => {
      capture(request);
      return HttpResponse.json(ME);
    }),
    http.get(MEMBERSHIPS_PATH, ({ request }) => {
      capture(request);
      return HttpResponse.json(status === 200 ? grants : { detail: "discovery_unavailable" }, {
        status,
      });
    }),
    http.get("/api/v1/dashboard/tenants", () =>
      HttpResponse.json({ detail: "tenant_header_required" }, { status: 400 }),
    ),
  );
  return requests;
}

function ServingProbe() {
  const serving = useServingOverview();
  return <output>{serving.data?.tenant_id}</output>;
}

function mount() {
  const root = createRootRoute({
    component: () => (
      <Shell>
        <ServingProbe />
      </Shell>
    ),
  });
  const router = createRouter({
    routeTree: root,
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <ThemeProvider>
      <QueryClientProvider client={client}>
        <AuthProvider>
          <RouterProvider router={router} />
        </AuthProvider>
      </QueryClientProvider>
    </ThemeProvider>,
  );
  return router;
}

function overview(tenant: string) {
  return {
    schema_version: "cnes-serving-v1",
    tenant_id: tenant,
    run_id: "test-run",
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

describe("descoberta AWS nas rotas reais", () => {
  test("guard_autentica_e_descobre_sem_tenant_antes_de_permitir_produto", async () => {
    const requests = discovery();
    await expect(guard()).resolves.toBeUndefined();
    expect(requests.map((r) => r.url)).toEqual([ME_PATH, MEMBERSHIPS_PATH]);
    expect(requests.every((r) => r.bearer === "Bearer test-token" && r.tenant === null)).toBe(true);
  });

  test("sem_membership_redireciona_para_acesso_pendente", async () => {
    discovery([]);
    await expect(guard()).rejects.toMatchObject({ options: { to: "/access-pending" } });
  });

  test("falha_503_de_descoberta_permanece_erro_e_nao_vira_lista_vazia", async () => {
    discovery([], 503);
    await expect(guard()).rejects.toMatchObject({
      status: 503,
      body: { detail: "discovery_unavailable" },
    });
  });

  test("token_invalido_redireciona_login_sem_descobrir", async () => {
    const requests = discovery();
    server.use(
      http.get(ME_PATH, () => HttpResponse.json({ detail: "token_invalid" }, { status: 401 })),
    );
    await expect(guard()).rejects.toMatchObject({ options: { to: "/login" } });
    expect(requests).toEqual([]);
  });

  test("oidc_legado_preserva_tenant_ids_sem_exigir_discovery_aws", async () => {
    const requests = discovery();
    server.use(
      http.get(ME_PATH, () =>
        HttpResponse.json({
          user_id: "legacy-user",
          email: "legacy@example.test",
          display_name: null,
          role: "gestor",
          tenant_ids: ["354130"],
          has_pending_request: false,
        }),
      ),
    );
    await expect(guard()).resolves.toBeUndefined();
    expect(requests).toEqual([]);
  });

  test("seletor_usa_nomes_reais_e_troca_header_do_produto", async () => {
    const requests = discovery();
    const tenants: (string | null)[] = [];
    server.use(
      http.get(SERVING_PATH, ({ request }) => {
        const tenant = request.headers.get("x-tenant-id");
        tenants.push(tenant);
        return HttpResponse.json(overview(tenant ?? "missing"));
      }),
    );
    mount();
    const selector = await screen.findByRole("combobox", { name: /município/i });
    expect(await screen.findByText("Presidente Epitácio", { exact: false })).toBeInTheDocument();
    await waitFor(() => expect(tenants).toContain("tenant-a"));
    if (selector.tagName === "SELECT") {
      await userEvent.selectOptions(selector, "tenant-z");
    } else {
      await userEvent.click(selector);
      await userEvent.click(await screen.findByRole("option", { name: /Campinas/ }));
    }
    await waitFor(() => expect(tenants.at(-1)).toBe("tenant-z"));
    expect(screen.queryByText(/IBGE|undefined/)).not.toBeInTheDocument();
    expect(requests.every((r) => r.bearer === "Bearer test-token" && r.tenant === null)).toBe(true);
  });
});
