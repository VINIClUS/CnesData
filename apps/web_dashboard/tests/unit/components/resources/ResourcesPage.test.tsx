import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { ResourcesPage } from "@/components/resources/ResourcesPage";

describe("ResourcesPage", () => {
  test("renderiza_h1_blocos_com_estagio_e_fluxo", async () => {
    const { container } = renderWithRouter(<ResourcesPage />, "/recursos");
    expect(
      await screen.findByRole("heading", { level: 1, name: "Conheça os recursos do CnesData" }),
    ).toBeInTheDocument();
    for (const id of ["organizacao", "conferencia", "indicadores", "fluxo"]) {
      expect(container.querySelector(`#${id}`)).not.toBeNull();
    }
    expect(screen.getAllByText("Em desenvolvimento").length).toBeGreaterThanOrEqual(7);
    expect(screen.getByText("Exemplo ilustrativo de divergência")).toBeInTheDocument();
  });

  test("cta_leva_para_contato_com_interesse_piloto_e_nao_divulga_precos", async () => {
    const { container } = renderWithRouter(<ResourcesPage />, "/recursos");
    await screen.findByRole("heading", { level: 1 });
    expect(screen.getByRole("link", { name: /Quero participar dos testes/ })).toHaveAttribute(
      "href",
      "/contato?interesse=piloto",
    );
    expect(container.querySelector('a[href^="/precos"]')).toBeNull();
  });
});
