import { expect, test } from "@playwright/test";

test.beforeEach(async ({ context }) => {
  await context.route("**/api/v1/dashboard/auth/me", (r) => r.fulfill({ status: 401, body: "" }));
});

test("landing_e_publica_sem_sessao", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/$/);
  await expect(page.getByRole("heading", { level: 1, name: /Transforme dados/ })).toBeVisible();
});

test("home_navega_para_recursos_pela_navbar", async ({ page }) => {
  await page.goto("/");
  await page
    .getByRole("navigation", { name: "Principal", exact: true })
    .getByRole("link", { name: "Recursos" })
    .click();
  await expect(page).toHaveURL(/\/recursos$/);
  await expect(
    page.getByRole("heading", { level: 1, name: "Conheça os recursos do CnesData" }),
  ).toBeVisible();
});

test("solicitar_acesso_leva_para_contato_com_interesse", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Solicitar acesso", exact: true }).click();
  await expect(page).toHaveURL(/\/contato\?interesse=acesso-antecipado$/);
  await expect(
    page.getByRole("heading", { level: 1, name: "Participe do acesso antecipado" }),
  ).toBeVisible();
});

test("entrar_leva_para_login", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Entrar" }).click();
  await expect(page).toHaveURL(/\/login$/);
});
