import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { AboutPage } from "@/components/about/AboutPage";

describe("AboutPage", () => {
  test("renderiza_h1_cinco_secoes_e_atribuicao", async () => {
    const { container } = renderWithRouter(<AboutPage />, "/sobre");
    expect(
      await screen.findByRole("heading", {
        level: 1,
        name: "Por que o CnesData está sendo desenvolvido",
      }),
    ).toBeInTheDocument();
    expect(container.querySelectorAll("article h2")).toHaveLength(5);
    expect(
      screen.getByText(
        "Projeto independente voltado a profissionais que trabalham com dados do CNES.",
      ),
    ).toBeInTheDocument();
    expect(screen.queryAllByRole("article")).toHaveLength(1);
  });

  test("cta_leva_para_contato_com_interesse_piloto", async () => {
    const { container } = renderWithRouter(<AboutPage />, "/sobre");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByRole("link", { name: /Quero participar dos testes/ })).toHaveAttribute(
      "href",
      "/contato?interesse=piloto",
    );
    expect(container.querySelector('a[href^="/precos"]')).toBeNull();
  });
});
