import { screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";
import { renderWithRouter } from "../../helpers/renderWithRouter";

import { AuthProvider } from "@/auth/AuthProvider";
import { LoginPage } from "@/components/login/LoginPage";

vi.mock("@/auth/oidc", () => ({
  startLogin: vi.fn(() => Promise.resolve()),
  getAccessToken: vi.fn(() => Promise.resolve("tok-1")),
}));

describe("LoginPage", () => {
  test("redireciona_para_agentes_quando_autenticado", async () => {
    const { router } = renderWithRouter(
      <AuthProvider>
        <LoginPage />
      </AuthProvider>,
      "/login",
    );
    await waitFor(() => expect(router.state.location.pathname).toBe("/agentes"));
  });

  test("renderiza_formulario_para_anonimo", async () => {
    server.use(
      http.get("/api/v1/dashboard/auth/me", () => new HttpResponse(null, { status: 401 })),
    );
    const { router } = renderWithRouter(
      <AuthProvider>
        <LoginPage />
      </AuthProvider>,
      "/login",
    );
    expect(await screen.findByRole("heading", { name: "Entrar na sua conta" })).toBeInTheDocument();
    expect(screen.getByLabelText("Usuário ou e-mail")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /^Entrar/ })).toBeInTheDocument();
    expect(router.state.location.pathname).toBe("/login");
  });
});
