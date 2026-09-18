import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { MarketingFooter } from "@/components/marketing/MarketingFooter";

describe("MarketingFooter", () => {
  test("variante_full_renderiza_links_e_assinatura", async () => {
    renderWithRouter(<MarketingFooter />);
    expect(await screen.findByRole("link", { name: "Início" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Recursos" })).toHaveAttribute("href", "/recursos");
    expect(screen.getByRole("link", { name: "Contato" })).toHaveAttribute("href", "/contato");
    expect(screen.getByRole("link", { name: "Termos" })).toHaveAttribute("href", "/termos");
    expect(screen.getByRole("link", { name: "Privacidade" })).toHaveAttribute(
      "href",
      "/privacidade",
    );
    expect(screen.getByText("Desenvolvido para a saúde pública brasileira.")).toBeInTheDocument();
  });

  test("nao_usa_mailto_para_termos_privacidade_ou_ajuda", async () => {
    const { container } = renderWithRouter(<MarketingFooter />);
    await screen.findByRole("link", { name: "Início" });
    const mailtos = Array.from(container.querySelectorAll('nav a[href^="mailto:"]'));
    expect(mailtos).toHaveLength(0);
  });

  test("nao_divulga_precos_em_nenhuma_variante", async () => {
    const full = renderWithRouter(<MarketingFooter />);
    await screen.findByRole("link", { name: "Início" });
    expect(full.container.querySelector('a[href^="/precos"]')).toBeNull();
    full.unmount();
    const compact = renderWithRouter(<MarketingFooter variant="compact" />);
    await screen.findByRole("link", { name: "Ajuda" });
    expect(compact.container.querySelector('a[href^="/precos"]')).toBeNull();
  });

  test("variante_compact_renderiza_links_de_login", async () => {
    renderWithRouter(<MarketingFooter variant="compact" />);
    expect(await screen.findByRole("link", { name: "Ajuda" })).toHaveAttribute("href", "/ajuda");
    expect(screen.getByRole("link", { name: "Contato" })).toHaveAttribute("href", "/contato");
    expect(screen.getByRole("link", { name: "Privacidade" })).toHaveAttribute(
      "href",
      "/privacidade",
    );
    expect(screen.queryByRole("link", { name: "Início" })).not.toBeInTheDocument();
  });

  test("renderiza_icones_sociais_com_aria_label", async () => {
    renderWithRouter(<MarketingFooter />);
    expect(await screen.findByRole("link", { name: "LinkedIn" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "GitHub" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "E-mail" })).toHaveAttribute(
      "href",
      "mailto:me@vinisantana.com",
    );
  });
});
