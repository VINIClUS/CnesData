import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { LegalPage } from "@/components/legal/LegalPage";
import { legal } from "@/i18n/legal";

describe("LegalPage", () => {
  test.each([
    ["privacidade", "Política de privacidade"],
    ["termos", "Termos de uso do site"],
    ["ajuda", "Precisa de ajuda?"],
  ] as const)("renderiza_%s_com_h1_e_secoes", async (slug, h1) => {
    const { container } = renderWithRouter(<LegalPage doc={legal[slug]} />, `/${slug}`);
    expect(await screen.findByRole("heading", { level: 1, name: h1 })).toBeInTheDocument();
    expect(container.querySelectorAll("article h2")).toHaveLength(legal[slug].sections.length);
    expect(screen.getByText(/Última atualização/)).toBeInTheDocument();
  });

  test("privacidade_marca_pontos_a_definir_e_lista_dados_coletados", async () => {
    renderWithRouter(<LegalPage doc={legal.privacidade} />, "/privacidade");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getAllByText("A definir").length).toBeGreaterThanOrEqual(3);
    expect(screen.getByText(/Obrigatórios: nome e e-mail/)).toBeInTheDocument();
    expect(screen.getByText(/Não coletamos CPF, CNPJ/)).toBeInTheDocument();
  });

  test("oferece_email_e_link_para_contato", async () => {
    renderWithRouter(<LegalPage doc={legal.ajuda} />, "/ajuda");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByRole("link", { name: /me@vinisantana\.com/ })).toHaveAttribute(
      "href",
      "mailto:me@vinisantana.com",
    );
    expect(screen.getByRole("link", { name: /Falar com o projeto/ })).toHaveAttribute(
      "href",
      "/contato",
    );
  });
});
