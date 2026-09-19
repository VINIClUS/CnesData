import { expect, test } from "@playwright/test";

test.beforeEach(async ({ context }) => {
  await context.route("**/api/v1/dashboard/auth/me", (r) =>
    r.fulfill({
      json: {
        user_id: "u-1",
        email: "g@m",
        display_name: "G",
        role: "gestor",
        tenant_ids: ["354130"],
        has_pending_request: false,
      },
    }),
  );
  await context.route("**/api/v1/dashboard/tenants", (r) =>
    r.fulfill({
      json: [{ ibge6: "354130", ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" }],
    }),
  );
});

test("toggle_muda_classe_html_para_dark", async ({ page }) => {
  await page.goto("/agentes");
  await page.getByRole("button", { name: /Tema Escuro/ }).click();
  const hasDark = await page.evaluate(() => document.documentElement.classList.contains("dark"));
  expect(hasDark).toBe(true);
});

test("preferencia_persiste_em_localStorage", async ({ page }) => {
  await page.goto("/agentes");
  await page.getByRole("button", { name: /Tema Escuro/ }).click();
  const stored = await page.evaluate(() => localStorage.getItem("cnesdata-theme"));
  expect(stored).toBe("dark");
});

test("tema_dark_aplicado_pelo_script_externo_antes_da_hidratacao", async ({ context, page }) => {
  await context.addInitScript(() => localStorage.setItem("cnesdata-theme", "dark"));
  const themeScript = page.waitForResponse((r) => r.url().endsWith("/theme-init.js"));
  await page.goto("/");
  expect((await themeScript).status()).toBe(200);
  expect(await page.evaluate(() => document.documentElement.classList.contains("dark"))).toBe(true);
});

test("tema_light_salvo_nao_aplica_classe_dark", async ({ context, page }) => {
  await context.addInitScript(() => localStorage.setItem("cnesdata-theme", "light"));
  await page.goto("/");
  expect(await page.evaluate(() => document.documentElement.classList.contains("dark"))).toBe(
    false,
  );
});
