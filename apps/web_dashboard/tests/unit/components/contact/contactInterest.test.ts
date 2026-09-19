import { describe, expect, test } from "vitest";

import { INTEREST_BY_INTERESSE, contatoSearchSchema } from "@/components/contact/contactInterest";

describe("contatoSearchSchema", () => {
  test("valor_desconhecido_vira_contato_sem_erro", () => {
    expect(contatoSearchSchema.parse({ interesse: "xyz" })).toEqual({ interesse: "contato" });
  });

  test("ausente_vira_contato", () => {
    expect(contatoSearchSchema.parse({})).toEqual({ interesse: "contato" });
  });

  test("aceita_os_tres_valores_validos", () => {
    for (const v of ["acesso-antecipado", "piloto", "contato"] as const) {
      expect(contatoSearchSchema.parse({ interesse: v }).interesse).toBe(v);
    }
  });

  test("mapeia_para_o_contrato_da_api", () => {
    expect(INTEREST_BY_INTERESSE).toEqual({
      "acesso-antecipado": "early_access",
      piloto: "pilot",
      contato: "contact",
    });
  });
});
