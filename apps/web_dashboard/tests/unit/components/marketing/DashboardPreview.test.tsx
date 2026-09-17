import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { DashboardPreview } from "@/components/marketing/preview/DashboardPreview";

describe("DashboardPreview", () => {
  test("renderiza_4_kpis_com_valores", () => {
    render(<DashboardPreview />);
    for (const v of ["17.842", "293.102", "48.771", "645"]) {
      expect(screen.getByText(v)).toBeInTheDocument();
    }
  });

  test("renderiza_legenda_do_donut", () => {
    render(<DashboardPreview />);
    expect(screen.getByText("UBS")).toBeInTheDocument();
    expect(screen.getByText("42%")).toBeInTheDocument();
    expect(screen.getByText("Outros")).toBeInTheDocument();
    expect(screen.getByText("25%")).toBeInTheDocument();
  });

  test("renderiza_eixo_de_meses", () => {
    render(<DashboardPreview />);
    expect(screen.getByText("Jan")).toBeInTheDocument();
    expect(screen.getByText("Set")).toBeInTheDocument();
  });
});
