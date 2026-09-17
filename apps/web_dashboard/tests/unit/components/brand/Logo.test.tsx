import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { Logo, LogoMark } from "@/components/brand/Logo";

describe("Logo", () => {
  test("renderiza_wordmark_cnes_data", () => {
    render(<Logo />);
    expect(screen.getByText("Cnes")).toBeInTheDocument();
    expect(screen.getByText("Data")).toBeInTheDocument();
  });

  test("renderiza_mark_com_tres_barras", () => {
    const { container } = render(<LogoMark size="lg" />);
    expect(container.querySelectorAll("rect")).toHaveLength(3);
  });
});
