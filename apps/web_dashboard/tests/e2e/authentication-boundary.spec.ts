import type { BrowserContext, Request } from "@playwright/test";

import { ACCESS_TOKEN, expect, test } from "./fixtures/oidc";

const ME_PATH = "/api/v1/dashboard/auth/me";
const TENANTS_PATH = "/api/v1/dashboard/tenants";
const LEGACY_ME = {
  user_id: "e2e-user",
  email: "gestor@example.test",
  display_name: "Gestor",
  role: "gestor",
  tenant_ids: ["354130"],
  has_pending_request: false,
};
const TENANTS = [{ ibge6: "354130", ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" }];

async function apiBoundary(
  context: BrowserContext,
  me: unknown = LEGACY_ME,
  acceptedToken = ACCESS_TOKEN,
) {
  const requests: Request[] = [];
  await context.route("**/api/v1/**", async (route) => {
    const request = route.request();
    requests.push(request);
    if (request.headers().authorization !== `Bearer ${acceptedToken}`) {
      await route.fulfill({ status: 401, json: { detail: "invalid_token" } });
      return;
    }
    const path = new URL(request.url()).pathname;
    const bodies: Record<string, unknown> = {
      [ME_PATH]: me,
      [TENANTS_PATH]: TENANTS,
      "/api/v1/dashboard/agents/status": { fetched_at: "2026-01-01T00:00:00Z", sources: [] },
      "/api/v1/dashboard/agents/runs": { runs: [] },
      "/api/v1/dashboard/access-requests/mine": [],
      "/api/v1/dashboard/access-requests/available-tenants": TENANTS,
    };
    await route.fulfill({ status: path in bodies ? 200 : 404, json: bodies[path] ?? {} });
  });
  return requests;
}

function expectDiscoveryBearer(requests: Request[]) {
  const discovery = requests.filter((r) =>
    [ME_PATH, TENANTS_PATH].includes(new URL(r.url()).pathname),
  );
  expect(discovery.length).toBeGreaterThan(0);
  for (const request of discovery) {
    expect(request.headers().authorization).toBe(`Bearer ${ACCESS_TOKEN}`);
    expect(request.headers()["x-tenant-id"]).toBeUndefined();
    expect(request.headers().cookie).toBeUndefined();
  }
}

test("http_200_sem_sessao_oidc_nao_autentica_nem_libera_produto", async ({
  context,
  page,
  oidc,
}) => {
  const requests: Request[] = [];
  await context.route("**/api/v1/**", (route) => {
    requests.push(route.request());
    return route.fulfill({ json: LEGACY_ME });
  });
  await page.goto("/agentes");
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.getByRole("button", { name: /^Entrar/ })).toBeVisible();
  expect(requests).toEqual([]);
  expect(oidc.exchanges()).toBe(0);
});

test("sessao_oidc_real_envia_bearer_e_preserva_produto_legado", async ({ context, page, oidc }) => {
  const requests = await apiBoundary(context);
  await oidc.login(page);
  await expect(page).toHaveURL(/\/agentes$/);
  await expect(page.getByRole("heading", { name: "Status dos agentes" })).toBeVisible();
  await expect(page.getByText("gestor@example.test", { exact: true })).toBeVisible();
  await expect(page.getByText("Presidente Epitácio", { exact: false })).toBeVisible();
  expectDiscoveryBearer(requests);
  const product = requests.filter((r) => new URL(r.url()).pathname.includes("/agents/"));
  expect(product.length).toBeGreaterThan(0);
  for (const request of product) {
    expect(request.headers().authorization).toBe(`Bearer ${ACCESS_TOKEN}`);
    expect(request.headers()["x-tenant-id"]).toBe("354130");
  }
});

test("sessao_oidc_sem_grants_preserva_signup_e_bearer", async ({ context, page, oidc }) => {
  const requests = await apiBoundary(context, { ...LEGACY_ME, tenant_ids: [] });
  await oidc.login(page);
  await expect(page).toHaveURL(/\/access-pending$/);
  await expect(page.getByRole("heading", { name: "Conta criada" })).toBeVisible();
  await page.getByRole("combobox").click();
  await expect(page.getByRole("option", { name: /Presidente Epitácio/ })).toBeVisible();
  expectDiscoveryBearer(requests);
  expect(requests.some((r) => new URL(r.url()).pathname.includes("/agents/"))).toBe(false);
});

test("http_401_com_sessao_oidc_nao_libera_produto", async ({ context, page, oidc }) => {
  const requests = await apiBoundary(context, LEGACY_ME, "different-accepted-token");
  const rejected = page.waitForResponse((r) => r.url().endsWith(ME_PATH) && r.status() === 401);
  await oidc.login(page);
  await rejected;
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.getByRole("heading", { name: "Status dos agentes" })).toHaveCount(0);
  expectDiscoveryBearer(requests);
  expect(requests.every((r) => new URL(r.url()).pathname === ME_PATH)).toBe(true);
});

test("token_oidc_expirado_nao_envia_bearer_nem_libera_produto", async ({ context, page, oidc }) => {
  const requests = await apiBoundary(context);
  await oidc.login(page, 0);
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.getByRole("button", { name: /^Entrar/ })).toBeVisible();
  expect(requests).toEqual([]);
});

test("reload_reusa_sessao_do_sdk_sem_novo_login", async ({ context, page, oidc }) => {
  const requests = await apiBoundary(context);
  await oidc.login(page);
  await expect(page.getByRole("heading", { name: "Status dos agentes" })).toBeVisible();
  const beforeReload = requests.length;
  await page.reload();
  await expect(page.getByRole("heading", { name: "Status dos agentes" })).toBeVisible();
  expect(oidc.exchanges()).toBe(1);
  expectDiscoveryBearer(requests.slice(beforeReload));
});
