import { describe, expect, test } from "vitest";

import { lagInMonths } from "@/lib/time";

describe("lagInMonths", () => {
  test("retorna_zero_quando_recente", () => {
    const now = new Date("2026-04-25T12:00:00Z");
    const ts = new Date("2026-04-25T06:00:00Z");
    expect(lagInMonths(ts, now)).toBe(0);
  });
  test("retorna_um_quando_30_dias", () => {
    const now = new Date("2026-04-25T12:00:00Z");
    const ts = new Date("2026-03-25T12:00:00Z");
    expect(lagInMonths(ts, now)).toBeGreaterThanOrEqual(1);
  });
});
