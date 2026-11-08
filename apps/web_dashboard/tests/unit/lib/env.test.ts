import { describe, expect, test } from "vitest";

import { parseEnv } from "@/lib/env";

const PRODUCTION_API_BASE_URL = "https://api.cnesdata.com.br/api/v1";
const COGNITO_DOMAIN = "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com";
const COGNITO_ISSUER = "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Example1";
const DOMAIN_REJECTED =
  /VITE_OIDC_DOMAIN: production_requires=https:\/\/<prefix>\.auth\.us-east-2\.amazoncognito\.com/;
const PRODUCTION_OIDC = {
  VITE_RUNTIME_POLICY: "production",
  VITE_API_BASE_URL: PRODUCTION_API_BASE_URL,
  VITE_OIDC_AUTHORITY: COGNITO_ISSUER,
  VITE_OIDC_CLIENT_ID: "dashboard-client-example",
  VITE_OIDC_REDIRECT_URI: "https://cnesdata.com.br/auth/callback",
};

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

describe("parseEnv do dominio oidc", () => {
  test("fica_ausente_por_padrao_e_quando_vazio", () => {
    expect(parseEnv({}).VITE_OIDC_DOMAIN).toBeUndefined();
    expect(parseEnv({ VITE_OIDC_DOMAIN: "" }).VITE_OIDC_DOMAIN).toBeUndefined();
  });

  test("fica_opcional_e_sem_validacao_fora_de_producao", () => {
    const env = parseEnv({ VITE_OIDC_DOMAIN: "https://auth.example.test" });

    expect(env.VITE_RUNTIME_POLICY).toBe("generic");
    expect(env.VITE_OIDC_DOMAIN).toBe("https://auth.example.test");
  });

  test("producao_aceita_dominio_de_prefixo_do_cognito_em_us_east_2", () => {
    const env = parseEnv({ ...PRODUCTION_OIDC, VITE_OIDC_DOMAIN: COGNITO_DOMAIN });

    expect(env.VITE_OIDC_DOMAIN).toBe(COGNITO_DOMAIN);
    expect(env.VITE_OIDC_AUTHORITY).toBe(COGNITO_ISSUER);
  });

  test("producao_exige_dominio_quando_o_oidc_esta_configurado", () => {
    expect(() => parseEnv(PRODUCTION_OIDC)).toThrow(DOMAIN_REJECTED);
  });

  test("producao_sem_oidc_configurado_nao_exige_dominio", () => {
    const env = parseEnv({
      VITE_RUNTIME_POLICY: "production",
      VITE_API_BASE_URL: PRODUCTION_API_BASE_URL,
    });

    expect(env.VITE_OIDC_DOMAIN).toBeUndefined();
  });

  test.each([
    "https://auth.cnesdata.com.br",
    "https://cnesdata-prod-example.auth.us-east-1.amazoncognito.com",
    "http://cnesdata-prod-example.auth.us-east-2.amazoncognito.com",
    `${COGNITO_DOMAIN}/`,
    `${COGNITO_DOMAIN}/oauth2/authorize`,
    `${COGNITO_DOMAIN}:443`,
    "https://CNESDATA.auth.us-east-2.amazoncognito.com",
    "https://-cnesdata.auth.us-east-2.amazoncognito.com",
    "https://cnes.data.auth.us-east-2.amazoncognito.com",
    `https://${"a".repeat(64)}.auth.us-east-2.amazoncognito.com`,
    `${COGNITO_DOMAIN}\n`,
  ])("producao_rejeita_dominio_%j", (domain) => {
    expect(() => parseEnv({ ...PRODUCTION_OIDC, VITE_OIDC_DOMAIN: domain })).toThrow(
      DOMAIN_REJECTED,
    );
  });

  test("producao_valida_dominio_mesmo_sem_oidc_configurado", () => {
    expect(() =>
      parseEnv({
        VITE_RUNTIME_POLICY: "production",
        VITE_API_BASE_URL: PRODUCTION_API_BASE_URL,
        VITE_OIDC_DOMAIN: "https://auth.cnesdata.com.br",
      }),
    ).toThrow(DOMAIN_REJECTED);
  });
});
