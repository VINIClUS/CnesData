import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";

describe("MarketingNavbar", () => {
  test("renderiza_links_publicos_e_acoes", async () => {
    renderWithRouter(<MarketingNavbar />);
    const nav = await screen.findByRole("navigation", { name: "Principal" });
    expect(nav).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Início" })).toHaveAttribute("href", "/");
    expect(screen.getByRole("link", { name: "Recursos" })).toHaveAttribute("href", "/recursos");
    expect(screen.getByRole("link", { name: "Sobre" })).toHaveAttribute("href", "/sobre");
    expect(screen.getByRole("link", { name: "Contato" })).toHaveAttribute("href", "/contato");
    expect(screen.getByRole("link", { name: "Entrar" })).toHaveAttribute("href", "/login");
    expect(screen.getByRole("link", { name: "Solicitar acesso" })).toHaveAttribute(
      "href",
      "/contato?interesse=acesso-antecipado",
    );
  });

  test("nao_divulga_precos", async () => {
    const { container } = renderWithRouter(<MarketingNavbar />);
    await screen.findByRole("navigation", { name: "Principal" });
    expect(screen.queryByRole("link", { name: "Preços" })).not.toBeInTheDocument();
    expect(container.querySelector('a[href^="/precos"]')).toBeNull();
    expect(container.querySelector('a[href^="mailto:"]')).toBeNull();
  });

  test("marca_recursos_como_ativo_em_recursos", async () => {
    renderWithRouter(<MarketingNavbar />, "/recursos");
    const recursos = await screen.findByRole("link", { name: "Recursos" });
    expect(recursos).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("link", { name: "Início" })).not.toHaveAttribute("aria-current");
  });

  test("contato_fica_ativo_mesmo_com_interesse_na_url", async () => {
    renderWithRouter(<MarketingNavbar />, "/contato?interesse=piloto");
    const contato = await screen.findByRole("link", { name: "Contato" });
    expect(contato).toHaveAttribute("aria-current", "page");
  });
});
