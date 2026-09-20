import {
  RouterProvider,
  createMemoryHistory,
  createRootRoute,
  createRouter,
} from "@tanstack/react-router";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, test, vi } from "vitest";

import { Shell } from "@/components/layout/Shell";
import { env } from "@/lib/env";
import { ThemeProvider } from "@/theme/ThemeProvider";

const mocks = vi.hoisted(() => ({
  logoutLocal: vi.fn<() => Promise<null>>(),
  logoutOidc: vi.fn<() => Promise<void>>(),
  refresh: vi.fn<() => Promise<void>>(),
  useTenants: vi.fn(),
}));

vi.mock("@/api/hooks/useTenants", () => ({ useTenants: mocks.useTenants }));
vi.mock("@/auth/local", () => ({ logoutLocal: mocks.logoutLocal }));
vi.mock("@/auth/oidc", () => ({ logout: mocks.logoutOidc }));
vi.mock("@/auth/useAuth", () => ({
  useAuth: () => ({
    user: { email: "gestor@example.com" },
    refresh: mocks.refresh,
  }),
}));

function renderShell() {
  const root = createRootRoute({ component: () => <Shell>Conteúdo</Shell> });
  const router = createRouter({
    routeTree: root,
    history: createMemoryHistory({ initialEntries: ["/"] }),
  });
  return render(
    <ThemeProvider>
      <RouterProvider router={router} />
    </ThemeProvider>,
  );
}

describe("Shell", () => {
  beforeEach(() => {
    env.VITE_AUTH_MODE = "oidc";
    mocks.logoutLocal.mockReset();
    mocks.logoutLocal.mockResolvedValue(null);
    mocks.logoutOidc.mockReset();
    mocks.logoutOidc.mockResolvedValue(undefined);
    mocks.refresh.mockReset();
    mocks.refresh.mockResolvedValue(undefined);
    mocks.useTenants.mockReturnValue({
      data: [{ nome: "Presidente Epitácio", uf: "SP", ibge6: "354130" }],
    });
  });

  test("logout_local_remove_sessao_e_atualiza_usuario", async () => {
    env.VITE_AUTH_MODE = "local";
    renderShell();

    await userEvent.click(await screen.findByRole("button", { name: "Sair" }));

    await waitFor(() => expect(mocks.logoutLocal).toHaveBeenCalledOnce());
    expect(mocks.refresh).toHaveBeenCalledOnce();
    expect(mocks.logoutOidc).not.toHaveBeenCalled();
  });

  test("logout_oidc_preserva_fluxo_oidc", async () => {
    mocks.useTenants.mockReturnValue({ data: undefined });
    renderShell();

    await userEvent.click(await screen.findByRole("button", { name: "Sair" }));

    await waitFor(() => expect(mocks.logoutOidc).toHaveBeenCalledOnce());
    expect(mocks.logoutLocal).not.toHaveBeenCalled();
    expect(mocks.refresh).not.toHaveBeenCalled();
  });
});
