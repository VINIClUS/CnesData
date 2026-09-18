import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, delay, http } from "msw";
import type { JsonBodyType } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { LeadForm } from "@/components/contact/LeadForm";

vi.mock("@/auth/oidc", () => ({ getAccessToken: vi.fn().mockResolvedValue(null) }));

const _URL = "/api/v1/public/leads";
const respond = (status: number, body: JsonBodyType = { detail: "x" }) =>
  http.post(_URL, () => HttpResponse.json(body, { status }));

async function fillRequired(user: ReturnType<typeof userEvent.setup>) {
  await user.type(screen.getByLabelText("Nome"), "Pessoa de teste");
  await user.type(screen.getByLabelText("E-mail"), "pessoa@example.org");
}

describe("LeadForm", () => {
  test("exige_nome_e_email_e_foca_primeiro_erro", async () => {
    const user = userEvent.setup();
    render(<LeadForm interesse="contato" />);
    await user.click(screen.getByRole("button", { name: "Enviar interesse" }));
    expect(screen.getByText("Informe seu nome.")).toBeInTheDocument();
    expect(screen.getByText("Informe seu e-mail.")).toBeInTheDocument();
    expect(screen.getByLabelText("Nome")).toHaveFocus();
    expect(screen.getByLabelText("Nome")).toHaveAttribute("aria-invalid", "true");
  });

  test("rejeita_email_invalido_e_limpa_erro_ao_editar", async () => {
    const user = userEvent.setup();
    render(<LeadForm interesse="contato" />);
    await user.type(screen.getByLabelText("Nome"), "Pessoa");
    await user.type(screen.getByLabelText("E-mail"), "invalido");
    await user.click(screen.getByRole("button", { name: "Enviar interesse" }));
    expect(screen.getByText("Informe um e-mail válido.")).toBeInTheDocument();
    await user.type(screen.getByLabelText("E-mail"), "@x.org");
    expect(screen.queryByText("Informe um e-mail válido.")).toBeNull();
  });

  test("preseleciona_interesse_conforme_modo_e_permite_alterar", async () => {
    const user = userEvent.setup();
    render(<LeadForm interesse="piloto" />);
    const select = screen.getByLabelText("Interesse");
    expect(select).toHaveValue("pilot");
    await user.selectOptions(select, "contact");
    expect(select).toHaveValue("contact");
  });

  test("envia_payload_completo_e_mostra_recebido_em_202", async () => {
    const user = userEvent.setup();
    let body: unknown;
    server.use(
      http.post(_URL, async ({ request }) => {
        body = await request.json();
        return HttpResponse.json({ status: "received" }, { status: 202 });
      }),
    );
    render(<LeadForm interesse="acesso-antecipado" />);
    await fillRequired(user);
    await user.type(screen.getByLabelText(/Instituição/), "Prefeitura X");
    await user.type(screen.getByLabelText(/Função/), "Cadastradora");
    await user.type(screen.getByLabelText(/O que você gostaria/), "Conferir vínculos");
    await user.click(screen.getByRole("button", { name: "Enviar interesse" }));
    expect(await screen.findByText(/Recebemos seu interesse no CnesData/)).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(/não cria uma conta/);
    expect(screen.queryByRole("button", { name: "Enviar interesse" })).toBeNull();
    expect(body).toEqual({
      name: "Pessoa de teste",
      email: "pessoa@example.org",
      interest: "early_access",
      organization: "Prefeitura X",
      role: "Cadastradora",
      message: "Conferir vínculos",
      municipality: "",
      source_path: "/contato",
      source_cta: "acesso-antecipado",
      privacy_notice_version: "1",
      newsletter_opt_in: false,
    });
  });

  test("ignora_duplo_clique_enquanto_envia", async () => {
    const user = userEvent.setup();
    let calls = 0;
    server.use(
      http.post(_URL, async () => {
        calls += 1;
        await delay(150);
        return HttpResponse.json({ status: "received" }, { status: 202 });
      }),
    );
    render(<LeadForm interesse="contato" />);
    await fillRequired(user);
    const button = screen.getByRole("button", { name: "Enviar interesse" });
    await user.click(button);
    expect(screen.getByRole("button", { name: "Enviando..." })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Enviando..." }));
    await screen.findByText(/Recebemos seu interesse/);
    expect(calls).toBe(1);
  });

  test.each([
    [422, /Alguns campos não foram aceitos/],
    [429, /Muitas tentativas/],
    [404, /serviço está indisponível/],
    [503, /serviço está indisponível/],
  ])("mostra_falha_em_%i_sem_sucesso", async (status, message) => {
    const user = userEvent.setup();
    server.use(respond(status));
    render(<LeadForm interesse="contato" />);
    await fillRequired(user);
    await user.click(screen.getByRole("button", { name: "Enviar interesse" }));
    expect(await screen.findByRole("status")).toHaveTextContent(message);
    expect(screen.queryByText(/Recebemos seu interesse/)).toBeNull();
    expect(screen.getByRole("button", { name: "Tentar novamente" })).toBeInTheDocument();
  });

  test("falha_de_rede_preserva_campos_e_oferece_email", async () => {
    const user = userEvent.setup();
    server.use(http.post(_URL, () => HttpResponse.error()));
    render(<LeadForm interesse="piloto" />);
    await fillRequired(user);
    await user.click(screen.getByRole("button", { name: "Enviar interesse" }));
    expect(await screen.findByRole("status")).toHaveTextContent(/falha de conexão/);
    expect(screen.getByLabelText("Nome")).toHaveValue("Pessoa de teste");
    expect(screen.getByLabelText("E-mail")).toHaveValue("pessoa@example.org");
    const mail = screen.getByRole("link", { name: "Enviar e-mail" });
    expect(mail.getAttribute("href")).toBe(
      "mailto:me@vinisantana.com?subject=Piloto%20do%20CnesData",
    );
  });

  test("tentar_novamente_reenvia_e_pode_ter_sucesso", async () => {
    const user = userEvent.setup();
    server.use(respond(503));
    render(<LeadForm interesse="contato" />);
    await fillRequired(user);
    await user.click(screen.getByRole("button", { name: "Enviar interesse" }));
    await screen.findByRole("button", { name: "Tentar novamente" });
    server.use(respond(202, { status: "received" }));
    await user.click(screen.getByRole("button", { name: "Tentar novamente" }));
    await waitFor(() =>
      expect(screen.getByRole("status")).toHaveTextContent(/Recebemos seu interesse/),
    );
  });
});
