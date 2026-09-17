import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, test } from "vitest";

import { ThemeIconButton } from "@/components/marketing/ThemeIconButton";
import { ThemeProvider } from "@/theme/ThemeProvider";

describe("ThemeIconButton", () => {
  test("alterna_para_dark_ao_clicar", async () => {
    render(
      <ThemeProvider>
        <ThemeIconButton />
      </ThemeProvider>,
    );
    const btn = screen.getByRole("button", { name: "Alternar tema" });
    expect(btn).toHaveAttribute("aria-pressed", "false");
    await userEvent.click(btn);
    expect(btn).toHaveAttribute("aria-pressed", "true");
    expect(document.documentElement.classList.contains("dark")).toBe(true);
    await userEvent.click(btn);
    expect(btn).toHaveAttribute("aria-pressed", "false");
  });
});
