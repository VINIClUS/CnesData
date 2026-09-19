import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { MarketingNavbar } from "@/components/marketing/MarketingNavbar";

describe("MarketingMobileNav", () => {
  test("abre_e_fecha_com_aria_expanded", async () => {
    const user = userEvent.setup();
    renderWithRouter(<MarketingNavbar />);
    const button = await screen.findByRole("button", { name: "Menu" });
    expect(button).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByRole("navigation", { name: "Menu de navegação" })).toBeNull();
    await user.click(button);
    expect(button).toHaveAttribute("aria-expanded", "true");
    const panel = screen.getByRole("navigation", { name: "Menu de navegação" });
    expect(panel).toBeVisible();
    expect(button).toHaveAttribute("aria-controls", panel.id);
  });

  test("lista_os_quatro_destinos_e_acoes_no_menu", async () => {
    const user = userEvent.setup();
    renderWithRouter(<MarketingNavbar />);
    await user.click(await screen.findByRole("button", { name: "Menu" }));
    const panel = screen.getByRole("navigation", { name: "Menu de navegação" });
    const hrefs = Array.from(panel.querySelectorAll("a")).map((a) => a.getAttribute("href"));
    expect(hrefs).toEqual([
      "/",
      "/recursos",
      "/sobre",
      "/contato",
      "/login",
      "/contato?interesse=acesso-antecipado",
    ]);
  });

  test("fecha_com_escape_e_devolve_foco_ao_botao", async () => {
    const user = userEvent.setup();
    renderWithRouter(<MarketingNavbar />);
    const button = await screen.findByRole("button", { name: "Menu" });
    await user.click(button);
    await user.keyboard("{Escape}");
    expect(button).toHaveAttribute("aria-expanded", "false");
    expect(button).toHaveFocus();
  });

  test("fecha_ao_selecionar_destino", async () => {
    const user = userEvent.setup();
    const { router } = renderWithRouter(<MarketingNavbar />);
    const button = await screen.findByRole("button", { name: "Menu" });
    await user.click(button);
    const panel = screen.getByRole("navigation", { name: "Menu de navegação" });
    await user.click(panel.querySelector('a[href="/sobre"]')!);
    expect(button).toHaveAttribute("aria-expanded", "false");
    expect(router.state.location.pathname).toBe("/sobre");
  });
});
