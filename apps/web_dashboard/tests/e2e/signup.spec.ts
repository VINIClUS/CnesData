import { authenticatedJson } from "./fixtures/api";
import { expect, test } from "./fixtures/oidc";

test.describe("signup flow", () => {
  test("user_sem_tenant_redireciona_para_access_pending", async ({ page, context, oidc }) => {
    await context.route("**/api/v1/dashboard/auth/me", (r) =>
      authenticatedJson(r, {
        json: {
          user_id: "u-1",
          email: "novo@m",
          display_name: "Novo",
          role: "gestor",
          tenant_ids: [],
          has_pending_request: false,
        },
      }),
    );
    await context.route("**/api/v1/dashboard/access-requests/mine", (r) =>
      authenticatedJson(r, { json: [] }),
    );
    await context.route("**/api/v1/dashboard/access-requests/available-tenants", (r) =>
      authenticatedJson(r, {
        json: [{ ibge6: "354130", ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" }],
      }),
    );
    await oidc.login(page);
    await expect(page).toHaveURL(/\/access-pending$/);
    await page.goto("/agentes");
    await expect(page).toHaveURL(/\/access-pending$/);
    await expect(page.getByText("Conta criada")).toBeVisible();
  });

  test("submete_request_e_ve_sucesso", async ({ page, context, oidc }) => {
    await context.route("**/api/v1/dashboard/auth/me", (r) =>
      authenticatedJson(r, {
        json: {
          user_id: "u-1",
          email: "novo@m",
          display_name: null,
          role: "gestor",
          tenant_ids: [],
          has_pending_request: false,
        },
      }),
    );
    await context.route("**/api/v1/dashboard/access-requests/mine", (r) =>
      authenticatedJson(r, { json: [] }),
    );
    await context.route("**/api/v1/dashboard/access-requests/available-tenants", (r) =>
      authenticatedJson(r, {
        json: [{ ibge6: "354130", ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" }],
      }),
    );
    await context.route("**/api/v1/dashboard/access-requests", (r) =>
      authenticatedJson(r, { json: { request_id: "req-1" }, status: 201 }),
    );
    await oidc.login(page);
    await expect(page).toHaveURL(/\/access-pending$/);
    await page.goto("/access-pending");
    await page.getByRole("combobox").click();
    await page.getByRole("option", { name: /Presidente Epitácio/ }).click();
    await page.getByLabel("Justificativa").fill("Sou gestor da SMS local");
    await page.getByRole("button", { name: "Solicitar acesso" }).click();
    await expect(page.getByText(/Solicitação enviada/)).toBeVisible();
  });
});
