import {
  InMemoryWebStorage,
  OidcClient,
  User,
  UserManager,
  WebStorageStateStore,
} from "oidc-client-ts";
import type { INavigator, UserManagerSettings } from "oidc-client-ts";
import { afterEach, describe, expect, test, vi } from "vitest";

import { buildOidcSettings, buildSignoutArgs } from "@/auth/oidcSettings";
import { parseEnv } from "@/lib/env";
import type { Env } from "@/lib/env";

const ORIGIN = "https://cnesdata.com.br";
const API_ORIGIN = "https://api.cnesdata.com.br";
const API_SCOPE = "openid profile email https://api.cnesdata.com.br/api.access";
const DOMAIN = "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com";
const AUTHORITY = "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Example1";
const CLIENT_ID = "dashboard-client-example";
const REDIRECT_URI = `${ORIGIN}/auth/callback`;
const NOT_CONFIGURED = "OIDC env not configured";

const PRODUCTION = {
  VITE_RUNTIME_POLICY: "production",
  VITE_API_BASE_URL: `${API_ORIGIN}/api/v1`,
  VITE_OIDC_AUTHORITY: AUTHORITY,
  VITE_OIDC_CLIENT_ID: CLIENT_ID,
  VITE_OIDC_REDIRECT_URI: REDIRECT_URI,
  VITE_OIDC_DOMAIN: DOMAIN,
};

const GENERIC = {
  VITE_OIDC_AUTHORITY: "http://localhost:8080/realms/cnesdata",
  VITE_OIDC_CLIENT_ID: "cnesdata-dashboard",
  VITE_OIDC_REDIRECT_URI: "http://localhost:5173/auth/callback",
};

afterEach(() => {
  vi.restoreAllMocks();
});

function memoryStore(): WebStorageStateStore {
  return new WebStorageStateStore({ store: new InMemoryWebStorage() });
}

function recordingNavigator(urls: string[]): INavigator {
  return {
    prepare: () =>
      Promise.resolve({
        navigate: ({ url }) => {
          urls.push(url);
          return Promise.resolve({ url });
        },
        close: () => undefined,
      }),
    callback: () => Promise.resolve(),
  };
}

function managerFor(settings: UserManagerSettings, urls: string[]): UserManager {
  const stores = { userStore: memoryStore(), stateStore: memoryStore() };
  return new UserManager({ ...settings, ...stores }, recordingNavigator(urls));
}

function onlyUrl(urls: string[]): URL {
  expect(urls).toHaveLength(1);
  return new URL(urls[0] ?? "");
}

async function authorizeUrl(settings: UserManagerSettings): Promise<URL> {
  const urls: string[] = [];
  await managerFor(settings, urls).signinRedirect();
  return onlyUrl(urls);
}

async function logoutUrl(env: Env, user?: User): Promise<URL> {
  const urls: string[] = [];
  const manager = managerFor(buildOidcSettings(env, ORIGIN), urls);
  if (user) await manager.storeUser(user);
  await manager.signoutRedirect(buildSignoutArgs(env, ORIGIN));
  return onlyUrl(urls);
}

function sessionUser(): User {
  return new User({
    id_token: "header.payload.signature",
    access_token: "access-token-example",
    token_type: "Bearer",
    profile: { sub: "user-1", iss: AUTHORITY, aud: CLIENT_ID, exp: 4102444800, iat: 1 },
  });
}

function endpointOf(url: URL): string {
  return `${url.origin}${url.pathname}`;
}

describe("oidc sob a politica de producao", () => {
  test("pede_o_scope_da_api_e_o_resource_em_producao", async () => {
    const settings = buildOidcSettings(parseEnv(PRODUCTION), ORIGIN);

    const url = await authorizeUrl(settings);

    expect(settings.scope).toBe(API_SCOPE);
    expect(settings.extraQueryParams).toEqual({ resource: API_ORIGIN });
    expect(url.searchParams.get("scope")).toBe(API_SCOPE);
    expect(url.searchParams.getAll("resource")).toEqual([API_ORIGIN]);
    expect(url.searchParams.get("client_id")).toBe(CLIENT_ID);
    expect(url.searchParams.get("redirect_uri")).toBe(REDIRECT_URI);
    expect(url.searchParams.get("response_type")).toBe("code");
    expect(url.searchParams.get("code_challenge_method")).toBe("S256");
  });

  test("usa_os_endpoints_padrao_do_cognito", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const settings = buildOidcSettings(parseEnv(PRODUCTION), ORIGIN);
    const client = new OidcClient({ ...settings, stateStore: memoryStore() });

    const url = await authorizeUrl(settings);

    expect(settings.metadata).toEqual({
      issuer: AUTHORITY,
      authorization_endpoint: `${DOMAIN}/oauth2/authorize`,
      token_endpoint: `${DOMAIN}/oauth2/token`,
      userinfo_endpoint: `${DOMAIN}/oauth2/userInfo`,
      jwks_uri: `${AUTHORITY}/.well-known/jwks.json`,
      end_session_endpoint: `${DOMAIN}/logout`,
    });
    expect(endpointOf(url)).toBe(`${DOMAIN}/oauth2/authorize`);
    await expect(client.metadataService.getTokenEndpoint()).resolves.toBe(`${DOMAIN}/oauth2/token`);
    expect(fetchSpy).not.toHaveBeenCalled();
  });

  test("encerra_a_sessao_no_logout_do_cognito_com_client_id_e_logout_uri", async () => {
    const url = await logoutUrl(parseEnv(PRODUCTION), sessionUser());

    expect(endpointOf(url)).toBe(`${DOMAIN}/logout`);
    expect(url.searchParams.get("id_token_hint")).toBe("header.payload.signature");
    expect(url.searchParams.getAll("client_id")).toEqual([CLIENT_ID]);
    expect(url.searchParams.getAll("logout_uri")).toEqual([ORIGIN]);
    expect(url.searchParams.has("post_logout_redirect_uri")).toBe(false);
    expect(url.searchParams.has("resource")).toBe(false);
  });

  test("encerra_a_sessao_sem_depender_de_id_token_hint", async () => {
    const url = await logoutUrl(parseEnv(PRODUCTION));

    expect(endpointOf(url)).toBe(`${DOMAIN}/logout`);
    expect([...url.searchParams.keys()].sort()).toEqual(["client_id", "logout_uri"]);
    expect(url.searchParams.get("client_id")).toBe(CLIENT_ID);
    expect(url.searchParams.get("logout_uri")).toBe(ORIGIN);
  });

  test("rejeita_suposicao_de_managed_login_v2", async () => {
    const settings = buildOidcSettings(parseEnv(PRODUCTION), ORIGIN);

    const url = await authorizeUrl(settings);

    expect(settings.metadataUrl).toBeUndefined();
    expect(settings.metadataSeed).toBeUndefined();
    expect(endpointOf(url)).not.toBe(`${DOMAIN}/login`);
    for (const param of ["prompt", "lang", "acr_values", "max_age"]) {
      expect(url.searchParams.has(param)).toBe(false);
    }
    expect(() =>
      parseEnv({ ...PRODUCTION, VITE_OIDC_DOMAIN: "https://auth.cnesdata.com.br" }),
    ).toThrow(/VITE_OIDC_DOMAIN: production_requires=/);
  });

  test("normaliza_a_barra_final_do_issuer_nos_metadados", () => {
    const env = parseEnv({ ...PRODUCTION, VITE_OIDC_AUTHORITY: `${AUTHORITY}/` });

    const settings = buildOidcSettings(env, ORIGIN);

    expect(settings.metadata?.issuer).toBe(AUTHORITY);
    expect(settings.metadata?.jwks_uri).toBe(`${AUTHORITY}/.well-known/jwks.json`);
  });

  test("falha_fechado_sem_dominio_mesmo_com_cliente_configurado", () => {
    const env: Env = { ...parseEnv(PRODUCTION), VITE_OIDC_DOMAIN: undefined };

    expect(() => buildOidcSettings(env, ORIGIN)).toThrow(NOT_CONFIGURED);
  });

  test("falha_no_logout_sem_client_id", () => {
    const env: Env = { ...parseEnv(PRODUCTION), VITE_OIDC_CLIENT_ID: undefined };

    expect(() => buildSignoutArgs(env, ORIGIN)).toThrow(NOT_CONFIGURED);
  });
});

describe("oidc fora da politica de producao", () => {
  test("mantem_scope_discovery_e_logout_atuais", () => {
    const settings = buildOidcSettings(parseEnv(GENERIC), "http://localhost:5173");

    expect(settings).toEqual({
      authority: GENERIC.VITE_OIDC_AUTHORITY,
      client_id: GENERIC.VITE_OIDC_CLIENT_ID,
      redirect_uri: GENERIC.VITE_OIDC_REDIRECT_URI,
      response_type: "code",
      scope: "openid profile email",
      post_logout_redirect_uri: "http://localhost:5173",
      loadUserInfo: false,
      automaticSilentRenew: true,
    });
    expect(buildSignoutArgs(parseEnv(GENERIC), "http://localhost:5173")).toEqual({});
  });

  test("ignora_o_dominio_cognito_fora_de_producao", () => {
    const env = parseEnv({ ...GENERIC, VITE_OIDC_DOMAIN: DOMAIN });

    const settings = buildOidcSettings(env, ORIGIN);

    expect(settings.scope).toBe("openid profile email");
    expect(settings.metadata).toBeUndefined();
    expect(settings.extraQueryParams).toBeUndefined();
  });

  test.each(["VITE_OIDC_AUTHORITY", "VITE_OIDC_CLIENT_ID", "VITE_OIDC_REDIRECT_URI"])(
    "falha_sem_%s",
    (key) => {
      const env = parseEnv({ ...GENERIC, [key]: "" });

      expect(() => buildOidcSettings(env, ORIGIN)).toThrow(NOT_CONFIGURED);
    },
  );
});
