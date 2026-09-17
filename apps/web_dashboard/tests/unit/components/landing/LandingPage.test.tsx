import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { LandingPage } from "@/components/landing/LandingPage";

describe("LandingPage", () => {
  test("renderiza_h1_e_ctas_para_login", async () => {
    renderWithRouter(<LandingPage />);
    expect(
      await screen.findByRole("heading", { level: 1, name: /Transforme dados do CNES/ }),
    ).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Comece agora/ })).toHaveAttribute("href", "/login");
    expect(screen.getByRole("link", { name: /Ver demonstração/ })).toHaveAttribute(
      "href",
      "/login",
    );
  });

  test("renderiza_secoes_recursos_e_sobre_com_ids", async () => {
    const { container } = renderWithRouter(<LandingPage />);
    await screen.findByRole("heading", { level: 1 });
    expect(container.querySelector("#recursos")).not.toBeNull();
    expect(container.querySelector("#sobre")).not.toBeNull();
  });

  test("renderiza_4_features_e_3_cards", async () => {
    renderWithRouter(<LandingPage />);
    expect(await screen.findByText("Dados centralizados")).toBeInTheDocument();
    expect(screen.getByText("Feito para a gestão pública")).toBeInTheDocument();
    expect(screen.getAllByRole("article")).toHaveLength(3);
    expect(screen.getByRole("link", { name: /Conheça todos os recursos/ })).toHaveAttribute(
      "href",
      "/precos",
    );
  });
});
