import { expect, test } from "@playwright/test";

test("usuario_anonimo_e_redirecionado_para_login", async ({ page }) => {
  await page.goto("/agentes");
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.getByRole("button", { name: "Entrar" })).toBeVisible();
});
