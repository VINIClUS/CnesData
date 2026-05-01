import { describe, expect, test } from "vitest";

import {
  formatBRL,
  formatCompetenciaBR,
  formatDateBR,
  formatLagMeses,
  formatRelativeTime,
} from "@/lib/format";

describe("formatBRL", () => {
  test("formata_centavos_em_reais", () => {
    expect(formatBRL(123456)).toMatch(/R\$\s?1\.234,56/);
  });
  test("formata_zero_centavos", () => {
    expect(formatBRL(0)).toMatch(/R\$\s?0,00/);
  });
});

describe("formatCompetenciaBR", () => {
  test("converte_yyyymm_em_mes_ano", () => {
    expect(formatCompetenciaBR(202604)).toBe("abr/2026");
  });
  test("converte_janeiro", () => {
    expect(formatCompetenciaBR(202601)).toBe("jan/2026");
  });
});

describe("formatLagMeses", () => {
  test("retorna_em_dia_quando_zero", () => {
    expect(formatLagMeses(0)).toBe("em dia");
  });
  test("retorna_um_mes", () => {
    expect(formatLagMeses(1)).toBe("1 mês atrás");
  });
  test("retorna_n_meses", () => {
    expect(formatLagMeses(3)).toBe("3 meses atrás");
  });
});

describe("formatRelativeTime", () => {
  test("formata_em_pt_br_relative", () => {
    const now = new Date("2026-04-25T12:00:00Z");
    const past = new Date("2026-04-25T06:00:00Z");
    expect(formatRelativeTime(past, now)).toMatch(/há/);
  });
});

describe("formatDateBR", () => {
  test("formata_dd_mm_yyyy", () => {
    expect(formatDateBR(new Date("2026-04-25T12:00:00Z"))).toBe("25/04/2026");
  });
});
