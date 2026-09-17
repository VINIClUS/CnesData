import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, test } from "vitest";

import { FaqList } from "@/components/pricing/FaqList";

const _ITEMS = [
  { question: "P1?", answer: "R1" },
  { question: "P2?", answer: "R2" },
];

describe("FaqList", () => {
  test("abre_e_fecha_pergunta_ao_clicar", async () => {
    render(<FaqList items={_ITEMS} />);
    const q1 = screen.getByRole("button", { name: "P1?" });
    expect(q1).toHaveAttribute("aria-expanded", "false");
    await userEvent.click(q1);
    expect(q1).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByText("R1")).toBeVisible();
    await userEvent.click(q1);
    expect(q1).toHaveAttribute("aria-expanded", "false");
  });

  test("abrir_outra_fecha_a_anterior", async () => {
    render(<FaqList items={_ITEMS} />);
    await userEvent.click(screen.getByRole("button", { name: "P1?" }));
    await userEvent.click(screen.getByRole("button", { name: "P2?" }));
    expect(screen.getByRole("button", { name: "P1?" })).toHaveAttribute("aria-expanded", "false");
    expect(screen.getByRole("button", { name: "P2?" })).toHaveAttribute("aria-expanded", "true");
  });
});
