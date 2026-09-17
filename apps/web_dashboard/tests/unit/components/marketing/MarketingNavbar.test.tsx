import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";

describe("MarketingNavbar", () => {
  test("renderiza_links_e_botoes_de_conta", async () => {
    renderWithRouter(<MarketingNavbar />);
    expect(await screen.findByRole("link", { name: "Início" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Recursos" })).toHaveAttribute("href", "/#recursos");
    expect(screen.getByRole("link", { name: "Preços" })).toHaveAttribute("href", "/precos");
    expect(screen.getByRole("link", { name: "Entrar" })).toHaveAttribute("href", "/login");
    expect(screen.getByRole("link", { name: "Criar conta" })).toHaveAttribute("href", "/login");
  });

  test("marca_precos_como_ativo_em_precos", async () => {
    renderWithRouter(<MarketingNavbar />, "/precos");
    const precos = await screen.findByRole("link", { name: "Preços" });
    expect(precos).toHaveAttribute("aria-current", "page");
    expect(screen.getByRole("link", { name: "Início" })).not.toHaveAttribute("aria-current");
  });

  test("contato_aponta_para_mailto", async () => {
    renderWithRouter(<MarketingNavbar />);
    const contato = await screen.findByRole("link", { name: "Contato" });
    expect(contato.getAttribute("href")).toMatch(/^mailto:/);
  });
});
