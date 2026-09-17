import { screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { renderWithRouter } from "../../helpers/renderWithRouter";

import { CtaBand } from "@/components/marketing/CtaBand";

describe("CtaBand", () => {
  test("renderiza_titulo_e_link_para_login", async () => {
    renderWithRouter(<CtaBand title="Pronto?" />);
    expect(await screen.findByRole("heading", { name: "Pronto?" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Criar minha conta/ })).toHaveAttribute(
      "href",
      "/login",
    );
  });
});
