import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { LandingPage } from "@/components/landing/LandingPage";

describe("LandingPage", () => {
  test("renderiza_h1_e_ctas_de_acesso_e_recursos", async () => {
    renderWithRouter(<LandingPage />);
    expect(
      await screen.findByRole("heading", { level: 1, name: /Transforme dados do CNES/ }),
    ).toBeInTheDocument();
    const accessLinks = screen.getAllByRole("link", { name: /Solicitar acesso antecipado/ });
    expect(accessLinks.length).toBeGreaterThanOrEqual(1);
    for (const link of accessLinks) {
      expect(link).toHaveAttribute("href", "/contato?interesse=acesso-antecipado");
    }
    expect(screen.getByRole("link", { name: /Conhecer os recursos/ })).toHaveAttribute(
      "href",
      "/recursos",
    );
  });

  test("identifica_desenvolvimento_e_previa_ilustrativa", async () => {
    renderWithRouter(<LandingPage />);
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getAllByText("Em desenvolvimento").length).toBeGreaterThan(0);
    expect(screen.getByText("Prévia ilustrativa da interface")).toBeInTheDocument();
  });

  test("renderiza_secoes_recursos_e_sobre_com_ids", async () => {
    const { container } = renderWithRouter(<LandingPage />);
    await screen.findByRole("heading", { level: 1 });
    expect(container.querySelector("#recursos")).not.toBeNull();
    expect(container.querySelector("#sobre")).not.toBeNull();
    expect(container.querySelector('a[href^="/precos"]')).toBeNull();
  });

  test("renderiza_4_features_e_3_cards", async () => {
    renderWithRouter(<LandingPage />);
    expect(await screen.findByText("Dados centralizados")).toBeInTheDocument();
    expect(screen.getByText("Feito para a gestão pública")).toBeInTheDocument();
    expect(screen.getAllByRole("article")).toHaveLength(3);
    expect(screen.getByRole("link", { name: /Conheça todos os recursos/ })).toHaveAttribute(
      "href",
      "/recursos",
    );
  });
});
