import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, test } from "vitest";

import { ThemeToggle } from "@/components/layout/ThemeToggle";
import { ThemeProvider } from "@/theme/ThemeProvider";

describe("ThemeToggle", () => {
  test("renderiza_3_botoes_e_destaca_ativo", () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>,
    );
    expect(screen.getByRole("button", { name: /Tema Claro/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Tema Escuro/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /Tema Sistema/ })).toBeInTheDocument();
    const sistema = screen.getByRole("button", { name: /Tema Sistema/ });
    expect(sistema).toHaveAttribute("aria-pressed", "true");
  });

  test("clique_muda_aria_pressed", async () => {
    render(
      <ThemeProvider>
        <ThemeToggle />
      </ThemeProvider>,
    );
    await userEvent.click(screen.getByRole("button", { name: /Tema Escuro/ }));
    const escuro = screen.getByRole("button", { name: /Tema Escuro/ });
    expect(escuro).toHaveAttribute("aria-pressed", "true");
  });
});
