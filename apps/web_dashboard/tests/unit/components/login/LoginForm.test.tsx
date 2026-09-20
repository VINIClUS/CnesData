import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { LoginForm } from "@/components/login/LoginForm";
import { login } from "@/i18n/login";
import { env } from "@/lib/env";

const startLogin = vi.hoisted(() => vi.fn<() => Promise<void>>());
const refresh = vi.hoisted(() => vi.fn<() => Promise<void>>());
vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue(null),
  startLogin,
}));
vi.mock("@/auth/useAuth", () => ({ useAuth: () => ({ refresh }) }));

describe("LoginForm", () => {
  beforeEach(() => {
    env.VITE_AUTH_MODE = "oidc";
    startLogin.mockReset();
    startLogin.mockResolvedValue(undefined);
    refresh.mockReset();
    refresh.mockResolvedValue(undefined);
  });

  test("chama_startLogin_com_login_hint_do_email", async () => {
    render(<LoginForm />);
    await userEvent.type(screen.getByLabelText("Usuário ou e-mail"), " g@m ");
    await userEvent.type(screen.getByLabelText("Senha"), "segredo");
    await userEvent.click(screen.getByRole("button", { name: /Entrar/ }));
    expect(startLogin).toHaveBeenCalledWith({ loginHint: "g@m" });
  });

  test("chama_startLogin_sem_hint_quando_email_vazio", async () => {
    render(<LoginForm />);
    await userEvent.click(screen.getByRole("button", { name: /Entrar/ }));
    expect(startLogin).toHaveBeenCalledWith({});
  });

  test("criar_conta_chama_startLogin_sem_hint", async () => {
    render(<LoginForm />);
    await userEvent.type(screen.getByLabelText("Usuário ou e-mail"), "g@m");
    await userEvent.click(screen.getByRole("button", { name: "Criar uma conta" }));
    expect(startLogin).toHaveBeenCalledWith({});
  });

  test("esqueceu_senha_chama_startLogin_com_hint", async () => {
    render(<LoginForm />);
    await userEvent.type(screen.getByLabelText("Usuário ou e-mail"), "g@m");
    await userEvent.click(screen.getByRole("button", { name: "Esqueceu sua senha?" }));
    expect(startLogin).toHaveBeenCalledWith({ loginHint: "g@m" });
  });

  test("mostra_alerta_quando_startLogin_falha", async () => {
    startLogin.mockRejectedValueOnce(new Error("OIDC env not configured"));
    render(<LoginForm />);
    await userEvent.click(screen.getByRole("button", { name: /Entrar/ }));
    expect(await screen.findByRole("alert")).toHaveTextContent(/Não foi possível iniciar/);
  });

  test("nao_envia_senha", async () => {
    render(<LoginForm />);
    await userEvent.type(screen.getByLabelText("Senha"), "segredo");
    await userEvent.click(screen.getByRole("button", { name: /Entrar/ }));
    const args = JSON.stringify(startLogin.mock.calls);
    expect(args).not.toContain("segredo");
  });

  test("login_local_envia_email_e_senha_para_api", async () => {
    env.VITE_AUTH_MODE = "local";
    let payload: unknown;
    let authorization: string | null = "unexpected";
    server.use(
      http.post("/api/v1/auth/local/login", async ({ request }) => {
        payload = await request.json();
        authorization = request.headers.get("authorization");
        return HttpResponse.json({
          user_id: "user-1",
          email: "g@m",
          tenant_id: "354130",
          role: "gestor",
        });
      }),
    );

    render(<LoginForm />);
    await userEvent.type(screen.getByLabelText("Usuário ou e-mail"), "g@m");
    await userEvent.type(screen.getByLabelText("Senha"), "correct-horse-battery");
    await userEvent.click(screen.getByRole("button", { name: /Entrar/ }));

    await waitFor(() => {
      expect(payload).toEqual({ email: "g@m", password: "correct-horse-battery" });
    });
    expect(authorization).toBeNull();
    expect(refresh).toHaveBeenCalledOnce();
  });

  test("login_local_mostra_erro_quando_api_rejeita", async () => {
    env.VITE_AUTH_MODE = "local";
    server.use(
      http.post("/api/v1/auth/local/login", () =>
        HttpResponse.json({ detail: "invalid_credentials" }, { status: 401 }),
      ),
    );

    render(<LoginForm />);
    await userEvent.type(screen.getByLabelText("Usuário ou e-mail"), "g@m");
    await userEvent.type(screen.getByLabelText("Senha"), "wrong-password");
    await userEvent.click(screen.getByRole("button", { name: /Entrar/ }));

    expect(await screen.findByRole("alert")).toHaveTextContent(login.form.error);
    expect(refresh).not.toHaveBeenCalled();
  });
});
