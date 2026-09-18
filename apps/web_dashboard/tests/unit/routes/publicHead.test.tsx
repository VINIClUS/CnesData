import { RouterProvider, createMemoryHistory, createRouter } from "@tanstack/react-router";
import type { AnyRoute } from "@tanstack/react-router";
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, test, vi } from "vitest";

import { Route as rootRoute } from "@/routes/__root";
import { Route as contatoRoute } from "@/routes/contato";
import { Route as indexRoute } from "@/routes/index";
import { Route as precosRoute } from "@/routes/precos";
import { Route as recursosRoute } from "@/routes/recursos";
import { Route as sobreRoute } from "@/routes/sobre";
import { ThemeProvider } from "@/theme/ThemeProvider";

vi.mock("@/auth/oidc", () => ({ getAccessToken: vi.fn().mockResolvedValue(null) }));

type FileRoute = AnyRoute & {
  update: (o: { id: string; path: string; getParentRoute: () => AnyRoute }) => AnyRoute;
};

function attach(path: string, file: AnyRoute): AnyRoute {
  return (file as FileRoute).update({ id: path, path, getParentRoute: () => rootRoute });
}

const routeTree = rootRoute.addChildren([
  attach("/", indexRoute),
  attach("/recursos", recursosRoute),
  attach("/sobre", sobreRoute),
  attach("/contato", contatoRoute),
  attach("/precos", precosRoute),
]);

function robotsMeta() {
  return document.head.querySelector('meta[name="robots"]')?.getAttribute("content") ?? null;
}

describe("head das páginas públicas", () => {
  test("precos_tem_noindex_e_titulo_e_nao_vaza_para_home", async () => {
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/precos"] }),
    });
    render(
      <ThemeProvider>
        <RouterProvider router={router} />
      </ThemeProvider>,
    );
    await screen.findByTestId("plan-basico");
    await waitFor(() => expect(document.title).toBe("Planos e preços | CnesData"));
    expect(robotsMeta()).toBe("noindex");
    await router.navigate({ to: "/" });
    await waitFor(() => expect(robotsMeta()).toBeNull());
    expect(document.title).toBe("CnesData — Dados do CNES para a gestão municipal");
    await router.navigate({ to: "/recursos" });
    await waitFor(() => expect(document.title).toBe("Recursos | CnesData"));
    expect(robotsMeta()).toBeNull();
  });

  test("contato_com_interesse_desconhecido_cai_no_padrao", async () => {
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/contato?interesse=xyz"] }),
    });
    render(
      <ThemeProvider>
        <RouterProvider router={router} />
      </ThemeProvider>,
    );
    expect(
      await screen.findByRole("heading", { level: 1, name: "Converse sobre o CnesData" }),
    ).toBeInTheDocument();
    expect(router.state.location.search).toEqual({ interesse: "contato" });
  });
});
