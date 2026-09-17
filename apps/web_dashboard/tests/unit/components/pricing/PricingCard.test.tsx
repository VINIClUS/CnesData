import { screen } from "@testing-library/react";
import { Users } from "lucide-react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { PricingCard } from "@/components/pricing/PricingCard";
import { pricing } from "@/i18n/pricing";

const [basico, profissional, enterprise] = pricing.plans;

describe("PricingCard", () => {
  test("renderiza_preco_de_e_parcelas", async () => {
    renderWithRouter(<PricingCard plan={basico} icon={Users} />);
    expect(await screen.findByText("R$ 149")).toBeInTheDocument();
    expect(screen.getByText(/De R\$ 2\.988/)).toBeInTheDocument();
    expect(screen.getByText("12x de")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Começar agora/ })).toHaveAttribute("href", "/login");
  });

  test("destaca_plano_mais_popular", async () => {
    renderWithRouter(<PricingCard plan={profissional} icon={Users} highlighted />);
    expect(await screen.findByText("Mais popular")).toBeInTheDocument();
    expect(screen.getByTestId("plan-profissional").className).toContain("border-primary");
  });

  test("enterprise_aponta_para_mailto", async () => {
    renderWithRouter(<PricingCard plan={enterprise} icon={Users} />);
    const link = await screen.findByRole("link", { name: /Falar com vendas/ });
    expect(link.getAttribute("href")).toMatch(/^mailto:/);
  });
});
