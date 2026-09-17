import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { MarketingFooter } from "@/components/marketing/MarketingFooter";

describe("MarketingFooter", () => {
  test("variante_full_renderiza_links_e_assinatura", async () => {
    renderWithRouter(<MarketingFooter />);
    expect(await screen.findByRole("link", { name: "Início" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Termos" })).toBeInTheDocument();
    expect(screen.getByText("Desenvolvido para a saúde pública brasileira.")).toBeInTheDocument();
  });

  test("variante_compact_renderiza_links_de_login", async () => {
    renderWithRouter(<MarketingFooter variant="compact" />);
    expect(await screen.findByRole("link", { name: "Ajuda" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Início" })).not.toBeInTheDocument();
  });

  test("renderiza_icones_sociais_com_aria_label", async () => {
    renderWithRouter(<MarketingFooter />);
    expect(await screen.findByRole("link", { name: "LinkedIn" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "GitHub" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "E-mail" })).toBeInTheDocument();
  });
});
