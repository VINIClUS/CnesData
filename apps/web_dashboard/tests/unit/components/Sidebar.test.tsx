import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRouter,
} from "@tanstack/react-router";
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { Sidebar } from "@/components/layout/Sidebar";

function renderSidebar(path: string) {
  const root = createRootRoute({ component: () => <Sidebar activePath={path} /> });
  const router = createRouter({
    routeTree: root,
    history: createMemoryHistory({ initialEntries: [path] }),
  });
  return render(<RouterProvider router={router} />);
}

describe("Sidebar", () => {
  test("renderiza_itens_v1_e_marca_v1_1_em_breve", async () => {
    renderSidebar("/agentes");
    expect(await screen.findByText("Status agentes")).toBeInTheDocument();
    expect(screen.getByText("Ativar agente")).toBeInTheDocument();
    const future = screen.getAllByText("em breve");
    expect(future.length).toBeGreaterThanOrEqual(3);
  });

  test("destaca_item_ativo_via_aria_current", async () => {
    renderSidebar("/agentes");
    const link = await screen.findByRole("link", { name: /Status agentes/ });
    expect(link).toHaveAttribute("aria-current", "page");
  });
});
