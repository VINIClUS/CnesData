import { TenantPill } from "@/components/layout/TenantPill";
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

describe("TenantPill", () => {
  test("renderiza_nome_uf_ibge", () => {
    render(<TenantPill nome="Presidente Epitácio" uf="SP" ibge6="354130" />);
    expect(screen.getByText(/Presidente Epitácio/)).toBeInTheDocument();
    expect(screen.getByText(/SP/)).toBeInTheDocument();
    expect(screen.getByText(/354130/)).toBeInTheDocument();
  });
});
