import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { CtaBand } from "@/components/marketing/CtaBand";

describe("CtaBand", () => {
  test("renderiza_titulo_e_link_para_contato_com_acesso_antecipado", async () => {
    renderWithRouter(<CtaBand title="Pronto?" />);
    expect(await screen.findByRole("heading", { name: "Pronto?" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Solicitar acesso antecipado/ })).toHaveAttribute(
      "href",
      "/contato?interesse=acesso-antecipado",
    );
    expect(screen.getByText(/Não cria conta/)).toBeInTheDocument();
  });

  test("aceita_interesse_piloto_e_textos_proprios", async () => {
    renderWithRouter(
      <CtaBand title="Testes" subtitle="Sub" button="Quero participar" interesse="piloto" />,
    );
    expect(await screen.findByText("Sub")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Quero participar/ })).toHaveAttribute(
      "href",
      "/contato?interesse=piloto",
    );
  });
});
