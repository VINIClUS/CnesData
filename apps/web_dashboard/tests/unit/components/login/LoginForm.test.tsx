import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, test, vi } from "vitest";

import { LoginForm } from "@/components/login/LoginForm";

const startLogin = vi.hoisted(() => vi.fn<() => Promise<void>>());
vi.mock("@/auth/oidc", () => ({ startLogin }));

describe("LoginForm", () => {
  beforeEach(() => {
    startLogin.mockReset();
    startLogin.mockResolvedValue(undefined);
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
});
