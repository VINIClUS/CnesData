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
      },
    }),
  );
  await context.route("**/api/v1/dashboard/tenants", (r) =>
    r.fulfill({
      json: [{ ibge6: "354130", ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" }],
    }),
  );
  await context.route("**/activate/confirm", (r) =>
    r.fulfill({
      json: { status: "approved", expires_in_seconds: 300 },
    }),
  );
});

test("gestor_aprova_user_code_e_ve_sucesso", async ({ page }) => {
  await page.goto("/activate");
  await page.getByLabel("Código de ativação").fill("WDJB-MJHT");
  await page.getByRole("button", { name: "Aprovar" }).click();
  await expect(page.getByText("Agente aprovado")).toBeVisible();
});
