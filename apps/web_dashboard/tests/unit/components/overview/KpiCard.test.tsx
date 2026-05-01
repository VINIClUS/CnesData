import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { KpiCard } from "@/components/overview/KpiCard";

describe("KpiCard", () => {
  test("renderiza_label_value_delta_up", () => {
    render(
      <KpiCard
        label="Faturamento"
        value="R$ 1.2M"
        delta={{ value: "4.2%", direction: "up" }}
        context="vs mar/2026"
      />,
    );
    expect(screen.getByText("Faturamento")).toBeInTheDocument();
    expect(screen.getByText("R$ 1.2M")).toBeInTheDocument();
    expect(screen.getByText(/▲ 4\.2%/)).toBeInTheDocument();
    expect(screen.getByText("vs mar/2026")).toBeInTheDocument();
  });

  test("renderiza_sem_delta_quando_undefined", () => {
    render(<KpiCard label="X" value="0" />);
    expect(screen.queryByText(/▲|▼/)).not.toBeInTheDocument();
  });

  test("usa_cor_yellow_quando_warn", () => {
    render(<KpiCard label="Estabs" value="7" delta={{ value: "2", direction: "warn" }} />);
    const delta = screen.getByText(/▲ 2/);
    expect(delta.className).toContain("text-yellow-600");
  });
});
