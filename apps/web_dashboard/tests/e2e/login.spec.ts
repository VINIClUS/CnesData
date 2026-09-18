import { expect, test } from "@playwright/test";

test("usuario_anonimo_e_redirecionado_para_login", async ({ page }) => {
  await page.goto("/agentes");
  await expect(page).toHaveURL(/\/login$/);
  await expect(page.getByRole("button", { name: "Entrar" })).toBeVisible();
});

test("submit_sem_oidc_configurado_mostra_alerta", async ({ page }) => {
  test.skip(Boolean(process.env.VITE_OIDC_AUTHORITY), "OIDC configurado: redirecionaria ao IdP");
  await page.goto("/login");
  await page.getByLabel("Usuário ou e-mail").fill("gestor@municipio.gov.br");
  await page.getByRole("button", { name: /^Entrar/ }).click();
  await expect(page.getByRole("alert")).toBeVisible();
});
