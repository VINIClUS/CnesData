import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRouter,
} from "@tanstack/react-router";
import { render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, test } from "vitest";

import { Sidebar } from "@/components/layout/Sidebar";
import { env } from "@/lib/env";

function renderSidebar(path: string) {
  const root = createRootRoute({ component: () => <Sidebar activePath={path} /> });
  const router = createRouter({
    routeTree: root,
    history: createMemoryHistory({ initialEntries: [path] }),
  });
  return render(<RouterProvider router={router} />);
}

describe("Sidebar", () => {
  beforeEach(() => {
    env.VITE_AUTH_MODE = "oidc";
  });

  afterEach(() => {
    env.VITE_AUTH_MODE = "oidc";
  });

  test("renderiza_itens_v1_e_marca_v1_1_em_breve", async () => {
    renderSidebar("/agentes");
    expect(await screen.findByText("Visão geral")).toBeInTheDocument();
    expect(screen.getByText("Status agentes")).toBeInTheDocument();
    expect(screen.getByText("Ativar agente")).toBeInTheDocument();
    const future = screen.getAllByText("em breve");
    expect(future.length).toBeGreaterThanOrEqual(2);
  });

  test("destaca_item_ativo_via_aria_current", async () => {
    renderSidebar("/agentes");
    const link = await screen.findByRole("link", { name: /Status agentes/ });
    expect(link).toHaveAttribute("aria-current", "page");
  });

  test("local_oculta_rotas_dependentes_do_backend_postgres", async () => {
    env.VITE_AUTH_MODE = "local";
    renderSidebar("/overview");

    expect(await screen.findByRole("link", { name: "Visão geral" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Status agentes" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Ativar agente" })).not.toBeInTheDocument();
  });
});
