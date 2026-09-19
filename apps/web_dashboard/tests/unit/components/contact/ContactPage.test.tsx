import { screen } from "@testing-library/react";
import { describe, expect, test, vi } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { ContactPage } from "@/components/contact/ContactPage";

vi.mock("@/auth/oidc", () => ({ getAccessToken: vi.fn().mockResolvedValue(null) }));

describe("ContactPage", () => {
  test.each([
    ["contato", "Converse sobre o CnesData", "contact"],
    ["acesso-antecipado", "Participe do acesso antecipado", "early_access"],
    ["piloto", "Tenho interesse em participar de um piloto", "pilot"],
  ] as const)("modo_%s_define_h1_e_interesse", async (interesse, h1, interest) => {
    renderWithRouter(<ContactPage interesse={interesse} />, `/contato?interesse=${interesse}`);
    expect(await screen.findByRole("heading", { level: 1, name: h1 })).toBeInTheDocument();
    expect(screen.getByLabelText("Interesse")).toHaveValue(interest);
  });

  test("mostra_orientacao_sobre_dados_sensiveis_e_nao_divulga_precos", async () => {
    const { container } = renderWithRouter(<ContactPage interesse="contato" />, "/contato");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByText(/Não envie dados de pacientes/)).toBeInTheDocument();
    expect(container.querySelector('a[href^="/precos"]')).toBeNull();
  });
});
