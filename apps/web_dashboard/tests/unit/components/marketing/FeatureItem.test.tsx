import { render, screen } from "@testing-library/react";
import { Database } from "lucide-react";
import { describe, expect, test } from "vitest";

import { FeatureItem } from "@/components/marketing/FeatureItem";

describe("FeatureItem", () => {
  test("renderiza_em_row", () => {
    render(<FeatureItem icon={Database} title="Titulo" description="Desc" />);
    expect(screen.getByRole("heading", { name: "Titulo" })).toBeInTheDocument();
    expect(screen.getByText("Desc")).toBeInTheDocument();
    expect(screen.queryByRole("article")).not.toBeInTheDocument();
  });

  test("renderiza_em_card", () => {
    render(<FeatureItem icon={Database} title="Titulo" description="Desc" layout="card" />);
    expect(screen.getByRole("article")).toBeInTheDocument();
  });
});
