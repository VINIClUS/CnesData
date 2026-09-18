import { expect, test } from "@playwright/test";

test.beforeEach(async ({ context }) => {
  await context.route("**/api/v1/dashboard/auth/me", (r) => r.fulfill({ status: 401, body: "" }));
});

test("landing_e_publica_sem_sessao", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveURL(/\/$/);
  await expect(page.getByRole("heading", { level: 1, name: /Transforme dados/ })).toBeVisible();
});

test("home_navega_para_precos_pela_navbar", async ({ page }) => {
  await page.goto("/");
  await page
    .getByRole("navigation", { name: "Principal" })
    .getByRole("link", { name: "Preços" })
    .click();
  await expect(page).toHaveURL(/\/precos$/);
  await expect(page.getByText("R$ 299")).toBeVisible();
});

test("criar_conta_leva_para_login", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("link", { name: "Criar conta" }).click();
  await expect(page).toHaveURL(/\/login$/);
});
