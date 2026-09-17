import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, test, vi } from "vitest";

import { PasswordField } from "@/components/login/PasswordField";

describe("PasswordField", () => {
  test("alterna_visibilidade_da_senha", async () => {
    const onChange = vi.fn();
    render(
      <PasswordField id="pw" label="Senha" value="" onChange={onChange} placeholder="Senha" />,
    );
    const input = screen.getByLabelText("Senha");
    expect(input).toHaveAttribute("type", "password");
    await userEvent.click(screen.getByRole("button", { name: "Mostrar senha" }));
    expect(input).toHaveAttribute("type", "text");
    await userEvent.click(screen.getByRole("button", { name: "Ocultar senha" }));
    expect(input).toHaveAttribute("type", "password");
  });

  test("propaga_valor_digitado", async () => {
    const onChange = vi.fn();
    render(
      <PasswordField id="pw" label="Senha" value="" onChange={onChange} placeholder="Senha" />,
    );
    await userEvent.type(screen.getByLabelText("Senha"), "a");
    expect(onChange).toHaveBeenCalledWith("a");
  });
});
