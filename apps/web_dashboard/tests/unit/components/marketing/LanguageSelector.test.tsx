import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { LanguageSelector } from "@/components/marketing/LanguageSelector";

describe("LanguageSelector", () => {
  test("mostra_portugues_brasil_como_valor", () => {
    render(<LanguageSelector />);
    expect(screen.getByRole("combobox", { name: "Idioma" })).toHaveTextContent(
      "Português (Brasil)",
    );
  });
});
