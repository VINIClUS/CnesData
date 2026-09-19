import { screen, within } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { PricingPage } from "@/components/pricing/PricingPage";

describe("PricingPage", () => {
  test("renderiza_3_planos_com_precos", async () => {
    renderWithRouter(<PricingPage />, "/precos");
    expect(await screen.findByText("R$ 149")).toBeInTheDocument();
    expect(screen.getByText("R$ 299")).toBeInTheDocument();
    expect(screen.getByText("R$ 799")).toBeInTheDocument();
    expect(screen.getAllByRole("article")).toHaveLength(3);
  });

  test("renderiza_4_itens_de_confianca_e_6_perguntas", async () => {
    renderWithRouter(<PricingPage />, "/precos");
    expect(await screen.findByText("Pagamento seguro")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Suporte especializado" })).toBeInTheDocument();
    const faq = within(document.querySelector("#faq") as HTMLElement);
    expect(faq.getAllByRole("button", { expanded: false })).toHaveLength(6);
  });
});
