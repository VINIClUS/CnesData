import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import { StageBadge } from "@/components/marketing/StageBadge";

describe("StageBadge", () => {
  test("usa_em_desenvolvimento_por_padrao", () => {
    render(<StageBadge />);
    expect(screen.getByText("Em desenvolvimento")).toBeInTheDocument();
  });

  test("renderiza_estagio_informado", () => {
    render(<StageBadge stage="planejado" />);
    expect(screen.getByText("Planejado")).toBeInTheDocument();
  });
});
