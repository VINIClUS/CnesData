import { describe, expect, test } from "vitest";

import { ACCESS_SEARCH, PUBLIC_NAV } from "@/components/marketing/marketingNavigation";

describe("marketingNavigation", () => {
  test("contem_apenas_os_quatro_destinos_publicos", () => {
    expect(PUBLIC_NAV.map((l) => l.to)).toEqual(["/", "/recursos", "/sobre", "/contato"]);
  });

  test("nao_contem_precos", () => {
    expect(PUBLIC_NAV.some((l) => l.to.startsWith("/precos"))).toBe(false);
  });

  test("solicitar_acesso_usa_interesse_de_acesso_antecipado", () => {
    expect(ACCESS_SEARCH).toEqual({ interesse: "acesso-antecipado" });
  });
});
