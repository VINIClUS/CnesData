import { parseEnv } from "@/lib/env";
import { describe, expect, test } from "vitest";

describe("parseEnv", () => {
  test("aceita_env_completo_e_valido", () => {
    const env = parseEnv({
      VITE_API_BASE_URL: "/api/v1",
      VITE_OIDC_AUTHORITY: "http://localhost:8080/realms/cnesdata",
      VITE_OIDC_CLIENT_ID: "cnesdata-dashboard",
      VITE_OIDC_REDIRECT_URI: "http://localhost:5173/auth/callback",
    });
    expect(env.VITE_API_BASE_URL).toBe("/api/v1");
  });

  test("aceita_apenas_api_base_url_em_dev", () => {
    const env = parseEnv({ VITE_API_BASE_URL: "/api/v1" });
    expect(env.VITE_OIDC_AUTHORITY).toBeUndefined();
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
});
