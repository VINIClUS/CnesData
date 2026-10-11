import { describe, expect, test } from "vitest";

import { parseEnv } from "@/lib/env";

const PRODUCTION_API_BASE_URL = "https://api.cnesdata.com.br/api/v1";

describe("parseEnv", () => {
  test("aceita_env_completo_e_valido", () => {
    const env = parseEnv({
      VITE_RUNTIME_POLICY: "generic",
      VITE_API_BASE_URL: "/api/v1",
      VITE_OIDC_AUTHORITY: "http://localhost:8080/realms/cnesdata",
      VITE_OIDC_CLIENT_ID: "cnesdata-dashboard",
      VITE_OIDC_REDIRECT_URI: "http://localhost:5173/auth/callback",
    });
    expect(env.VITE_RUNTIME_POLICY).toBe("generic");
    expect(env.VITE_API_BASE_URL).toBe("/api/v1");
  });

  test("aceita_apenas_api_base_url_em_dev", () => {
    const env = parseEnv({ VITE_API_BASE_URL: "/api/v1" });
    expect(env.VITE_RUNTIME_POLICY).toBe("generic");
    expect(env.VITE_AUTH_MODE).toBe("oidc");
    expect(env.VITE_OIDC_AUTHORITY).toBeUndefined();
  });

  test("aceita_modo_de_autenticacao_local", () => {
    const env = parseEnv({
      VITE_RUNTIME_POLICY: "generic",
      VITE_API_BASE_URL: "/api/v1",
      VITE_AUTH_MODE: "local",
    });

    expect(env.VITE_AUTH_MODE).toBe("local");
    expect(env.VITE_API_BASE_URL).toBe("/api/v1");
  });

  test("rejeita_modo_de_autenticacao_desconhecido", () => {
    expect(() => parseEnv({ VITE_AUTH_MODE: "unknown" })).toThrow();
  });

  test("rejeita_authority_sem_url_valida", () => {
    expect(() =>
      parseEnv({
        VITE_API_BASE_URL: "/api/v1",
        VITE_OIDC_AUTHORITY: "not-a-url",
        VITE_OIDC_CLIENT_ID: "x",
        VITE_OIDC_REDIRECT_URI: "http://localhost",
      }),
    ).toThrow(/VITE_OIDC_AUTHORITY/);
  });

  test("rejeita_api_base_url_vazio", () => {
    expect(() => parseEnv({ VITE_API_BASE_URL: "" })).toThrow();
  });

  test("aplica_default_quando_api_base_url_ausente", () => {
    const env = parseEnv({});
    expect(env.VITE_RUNTIME_POLICY).toBe("generic");
    expect(env.VITE_API_BASE_URL).toBe("/api/v1");
  });

  test("trata_variaveis_oidc_vazias_como_ausentes", () => {
    // Regressão: Docker ARG sem valor passado no build vira ENV="" (string vazia),
    // não variável ausente — mesma armadilha do os.getenv(key, default) em #238.
    // z.string().url().optional() rejeita "" (não é URL nem undefined), então um
    // build sem os 3 build-args OIDC crashava o app inteiro em vez de degradar
    // para "OIDC não configurado".
    const env = parseEnv({
      VITE_API_BASE_URL: "/api/v1",
      VITE_OIDC_AUTHORITY: "",
      VITE_OIDC_CLIENT_ID: "",
      VITE_OIDC_REDIRECT_URI: "",
    });
    expect(env.VITE_OIDC_AUTHORITY).toBeUndefined();
    expect(env.VITE_OIDC_CLIENT_ID).toBeUndefined();
    expect(env.VITE_OIDC_REDIRECT_URI).toBeUndefined();
  });

  test("trata_politica_de_runtime_vazia_como_generica", () => {
    const env = parseEnv({ VITE_RUNTIME_POLICY: "", VITE_API_BASE_URL: "/api/v1" });

    expect(env.VITE_RUNTIME_POLICY).toBe("generic");
  });

  test("rejeita_politica_de_runtime_desconhecida", () => {
    expect(() => parseEnv({ VITE_RUNTIME_POLICY: "prod" })).toThrow(/VITE_RUNTIME_POLICY/);
  });
});

describe("parseEnv em politica de producao", () => {
  test("aceita_base_absoluta_exata_com_oidc", () => {
    const env = parseEnv({
      VITE_RUNTIME_POLICY: "production",
      VITE_API_BASE_URL: PRODUCTION_API_BASE_URL,
      VITE_AUTH_MODE: "oidc",
    });

    expect(env.VITE_RUNTIME_POLICY).toBe("production");
    expect(env.VITE_API_BASE_URL).toBe(PRODUCTION_API_BASE_URL);
  });

  test("aceita_uma_barra_final_na_base", () => {
    const env = parseEnv({
      VITE_RUNTIME_POLICY: "production",
      VITE_API_BASE_URL: `${PRODUCTION_API_BASE_URL}/`,
    });

    expect(env.VITE_AUTH_MODE).toBe("oidc");
  });

  test.each([
    "/api/v1",
    "https://wrong.example/api/v1",
    "https://api.cnesdata.com.br",
    "https://api.cnesdata.com.br/api/v2",
    "http://api.cnesdata.com.br/api/v1",
    `${PRODUCTION_API_BASE_URL}?tenant=1`,
    `${PRODUCTION_API_BASE_URL}#fragmento`,
    `${PRODUCTION_API_BASE_URL}//`,
  ])("rejeita_base_%s", (base) => {
    expect(() => parseEnv({ VITE_RUNTIME_POLICY: "production", VITE_API_BASE_URL: base })).toThrow(
      /VITE_API_BASE_URL: production_requires=https:\/\/api\.cnesdata\.com\.br\/api\/v1/,
    );
  });

  test("rejeita_base_default_relativa_quando_ausente", () => {
    expect(() => parseEnv({ VITE_RUNTIME_POLICY: "production" })).toThrow(/VITE_API_BASE_URL/);
  });

  test("rejeita_modo_de_autenticacao_local", () => {
    expect(() =>
      parseEnv({
        VITE_RUNTIME_POLICY: "production",
        VITE_API_BASE_URL: PRODUCTION_API_BASE_URL,
        VITE_AUTH_MODE: "local",
      }),
    ).toThrow(/VITE_AUTH_MODE: production_requires=oidc/);
  });
});
