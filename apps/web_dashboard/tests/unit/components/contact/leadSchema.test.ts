import { describe, expect, test } from "vitest";

import { validateLead } from "@/components/contact/leadSchema";
import type { LeadValues } from "@/components/contact/leadSchema";

const base: LeadValues = {
  name: "Pessoa",
  email: "pessoa@example.org",
  interest: "contact",
  organization: "",
  role: "",
  message: "",
};

describe("validateLead", () => {
  test("aceita_campos_obrigatorios_e_normaliza_espacos", () => {
    const r = validateLead({ ...base, name: "  Pessoa  " });
    expect(r.ok && r.lead.name).toBe("Pessoa");
  });

  test("exige_nome_e_email_com_mensagens_em_portugues", () => {
    const r = validateLead({ ...base, name: "", email: "" });
    expect(r.ok).toBe(false);
    if (r.ok) return;
    expect(r.errors.name).toBe("Informe seu nome.");
    expect(r.errors.email).toBe("Informe seu e-mail.");
  });

  test("rejeita_email_invalido", () => {
    const r = validateLead({ ...base, email: "nao-e-email" });
    expect(!r.ok && r.errors.email).toBe("Informe um e-mail válido.");
  });

  test.each([
    ["name", 121, "O nome deve ter até 120 caracteres."],
    ["organization", 201, "A instituição deve ter até 200 caracteres."],
    ["role", 121, "A função deve ter até 120 caracteres."],
    ["message", 2001, "A mensagem deve ter até 2000 caracteres."],
  ] as const)("rejeita_%s_acima_do_limite", (field, size, message) => {
    const r = validateLead({ ...base, [field]: "a".repeat(size) });
    expect(!r.ok && r.errors[field]).toBe(message);
  });

  test("rejeita_email_acima_de_254", () => {
    const r = validateLead({ ...base, email: `${"a".repeat(250)}@x.org` });
    expect(!r.ok && r.errors.email).toBe("O e-mail deve ter até 254 caracteres.");
  });
});
