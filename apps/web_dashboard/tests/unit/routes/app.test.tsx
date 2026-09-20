import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider, createMemoryHistory, createRouter } from "@tanstack/react-router";
import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test } from "vitest";

import { server } from "../../mocks/server";

import { AuthProvider } from "@/auth/AuthProvider";
import { env } from "@/lib/env";
import { routeTree } from "@/routeTree.gen";
import { ThemeProvider } from "@/theme/ThemeProvider";

const LOCAL_PRINCIPAL = {
  user_id: "user-1",
  email: "g@m",
  tenant_id: "354130",
  role: "gestor",
};

const SERVING_OVERVIEW = {
  schema_version: "cnes-serving-v1",
  tenant_id: "354130",
  run_id: "run-1",
  generated_at: "2026-01-31T23:59:59Z",
  competencia: "2026-01",
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

describe("guardião das rotas autenticadas", () => {
  test("sessao_local_ausente_redireciona_para_login", async () => {
    env.VITE_AUTH_MODE = "local";
    server.use(
      http.get("/api/v1/auth/local/me", () =>
        HttpResponse.json({ detail: "not authenticated" }, { status: 401 }),
      ),
    );
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/overview"] }),
    });

    render(
      <ThemeProvider>
        <AuthProvider>
          <RouterProvider router={router} />
        </AuthProvider>
      </ThemeProvider>,
    );

    await waitFor(() => expect(router.state.location.pathname).toBe("/login"));
  });

  test("sessao_local_valida_permite_rota_autenticada", async () => {
    env.VITE_AUTH_MODE = "local";
    server.use(
      http.get("/api/v1/auth/local/me", () => HttpResponse.json(LOCAL_PRINCIPAL)),
      http.get("/api/v1/dashboard/tenants", () =>
        HttpResponse.json([{ ibge6: "354130", ibge7: "3541300", nome: "Epitácio", uf: "SP" }]),
      ),
      http.get("/api/v1/dashboard/serving/cnes/overview", () =>
        HttpResponse.json(SERVING_OVERVIEW),
      ),
    );
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/overview"] }),
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <ThemeProvider>
        <QueryClientProvider client={queryClient}>
          <AuthProvider>
            <RouterProvider router={router} />
          </AuthProvider>
        </QueryClientProvider>
      </ThemeProvider>,
    );

    expect(await screen.findByRole("heading", { name: "Visão geral" })).toBeInTheDocument();
    expect(router.state.location.pathname).toBe("/overview");
  });

  test("perfil_local_nao_expoe_status_de_agentes", async () => {
    env.VITE_AUTH_MODE = "local";
    server.use(http.get("/api/v1/auth/local/me", () => HttpResponse.json(LOCAL_PRINCIPAL)));
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/agentes"] }),
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <ThemeProvider>
        <QueryClientProvider client={queryClient}>
          <AuthProvider>
            <RouterProvider router={router} />
          </AuthProvider>
        </QueryClientProvider>
      </ThemeProvider>,
    );

    expect(
      await screen.findByText("O status dos agentes está disponível no perfil OIDC."),
    ).toBeInTheDocument();
  });

  test("perfil_local_nao_expoe_ativacao_de_agentes", async () => {
    env.VITE_AUTH_MODE = "local";
    server.use(http.get("/api/v1/auth/local/me", () => HttpResponse.json(LOCAL_PRINCIPAL)));
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/activate"] }),
    });
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    render(
      <ThemeProvider>
        <QueryClientProvider client={queryClient}>
          <AuthProvider>
            <RouterProvider router={router} />
          </AuthProvider>
        </QueryClientProvider>
      </ThemeProvider>,
    );

    expect(
      await screen.findByText("A ativação de agentes está disponível no perfil OIDC."),
    ).toBeInTheDocument();
  });
});
