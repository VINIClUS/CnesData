import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";

import { expect, test as base } from "@playwright/test";
import type { BrowserContext, Page, Route } from "@playwright/test";

export const ACCESS_TOKEN = "e2e-disposable-access-token";
const CLIENT_ID = "cnesdata-e2e";
const CODE = "e2e-disposable-code";

type Authorization = { challenge: string; nonce: string | null };
type OidcBoundary = {
  login: (page: Page, expiresIn?: number) => Promise<void>;
  exchanges: () => number;
};

async function appServer(port: number, provide: (origin: string) => Promise<void>) {
  const origin = `http://localhost:${port}`;
  const app = spawn("bun", ["run", "dev", "--port", String(port), "--strictPort"], {
    cwd: fileURLToPath(new URL("../../../", import.meta.url)),
    env: {
      ...process.env,
      VITE_RUNTIME_POLICY: "generic",
      VITE_AUTH_MODE: "oidc",
      VITE_API_BASE_URL: "/api/v1",
      VITE_OIDC_AUTHORITY: `${origin}/test-idp`,
      VITE_OIDC_CLIENT_ID: CLIENT_ID,
      VITE_OIDC_REDIRECT_URI: `${origin}/auth/callback`,
      VITE_OIDC_DOMAIN: "",
    },
    stdio: "ignore",
  });
  try {
    await expect
      .poll(() =>
        fetch(origin)
          .then((r) => r.status)
          .catch(() => 0),
      )
      .toBe(200);
    await provide(origin);
  } finally {
    const stopped = new Promise<void>((resolve) => app.once("exit", () => resolve()));
    if (app.exitCode === null && app.signalCode === null) {
      app.kill("SIGTERM");
      await stopped;
    }
  }
}

function discovery(origin: string) {
  const issuer = `${origin}/test-idp`;
  return {
    issuer,
    authorization_endpoint: `${issuer}/authorize`,
    token_endpoint: `${issuer}/token`,
    jwks_uri: `${issuer}/jwks`,
    response_types_supported: ["code"],
    subject_types_supported: ["public"],
    id_token_signing_alg_values_supported: ["RS256"],
  };
}

function idToken(origin: string, nonce: string | null) {
  const now = Math.floor(Date.now() / 1000);
  const claims = {
    iss: `${origin}/test-idp`,
    aud: CLIENT_ID,
    sub: "e2e-user",
    email: "gestor@example.test",
    iat: now,
    exp: now + 3600,
    ...(nonce ? { nonce } : {}),
  };
  const encoded = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encoded({ alg: "RS256", typ: "JWT" })}.${encoded(claims)}.test-signature`;
}

async function authorize(route: Route, origin: string): Promise<Authorization> {
  const query = new URL(route.request().url()).searchParams;
  expect(query.get("client_id")).toBe(CLIENT_ID);
  expect(query.get("response_type")).toBe("code");
  expect(query.get("redirect_uri")).toBe(`${origin}/auth/callback`);
  expect(query.get("code_challenge_method")).toBe("S256");
  const challenge = query.get("code_challenge");
  const state = query.get("state");
  expect(challenge).toBeTruthy();
  expect(state).toBeTruthy();
  const callback = new URL(`${origin}/auth/callback`);
  callback.searchParams.set("state", state!);
  callback.searchParams.set("code", CODE);
  await route.fulfill({ status: 302, headers: { location: callback.href } });
  return { challenge: challenge!, nonce: query.get("nonce") };
}

async function exchange(
  route: Route,
  origin: string,
  authorization: Authorization | null,
  expiresIn: number,
) {
  expect(authorization).not.toBeNull();
  expect(route.request().method()).toBe("POST");
  const form = new URLSearchParams(route.request().postData() ?? "");
  expect(form.get("grant_type")).toBe("authorization_code");
  expect(form.get("code")).toBe(CODE);
  expect(form.get("client_id")).toBe(CLIENT_ID);
  expect(form.get("redirect_uri")).toBe(`${origin}/auth/callback`);
  const verifier = form.get("code_verifier");
  expect(verifier).toBeTruthy();
  expect(createHash("sha256").update(verifier!).digest("base64url")).toBe(authorization!.challenge);
  await route.fulfill({
    json: {
      access_token: ACCESS_TOKEN,
      token_type: "Bearer",
      expires_in: expiresIn,
      scope: "openid profile email",
      id_token: idToken(origin, authorization!.nonce),
    },
  });
}

async function oidcBoundary(context: BrowserContext, origin: string): Promise<OidcBoundary> {
  let authorization: Authorization | null = null;
  let expiresIn = 3600;
  let exchanges = 0;
  await context.route("**/test-idp/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path.endsWith("/.well-known/openid-configuration")) {
      await route.fulfill({ json: discovery(origin) });
    } else if (path.endsWith("/authorize")) {
      authorization = await authorize(route, origin);
    } else if (path.endsWith("/token") && exchanges > 0) {
      await route.fulfill({ status: 400, json: { error: "invalid_grant" } });
    } else if (path.endsWith("/token")) {
      exchanges++;
      await exchange(route, origin, authorization, expiresIn);
    } else {
      throw new Error(`unexpected_oidc_path=${path}`);
    }
  });
  await context.route("**/api/v1/**", (route) => route.fulfill({ status: 404, json: {} }));
  return {
    exchanges: () => exchanges,
    login: async (page, requestedExpiry = 3600) => {
      expiresIn = requestedExpiry;
      await page.goto(`${origin}/login`);
      await page.getByLabel("Usuário ou e-mail").fill("gestor@example.test");
      await page.getByRole("button", { name: /^Entrar/ }).click();
      await expect.poll(() => exchanges).toBe(1);
      await expect
        .poll(() =>
          page.evaluate(() => Object.keys(sessionStorage).some((k) => k.startsWith("oidc.user:"))),
        )
        .toBe(true);
    },
  };
}

export const test = base.extend<{ oidc: OidcBoundary }, { oidcOrigin: string }>({
  oidcOrigin: [
    async ({ browserName: _browserName }, provide, workerInfo) =>
      appServer(15200 + workerInfo.parallelIndex, provide),
    { scope: "worker" },
  ],
  baseURL: async ({ oidcOrigin }, provide) => provide(oidcOrigin),
  oidc: async ({ context, oidcOrigin }, provide) =>
    provide(await oidcBoundary(context, oidcOrigin)),
});

export { expect } from "@playwright/test";
