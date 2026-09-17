import { expect, test } from "@playwright/test";

test.beforeEach(async ({ context }) => {
  await context.route("**/api/v1/dashboard/auth/me", (r) => r.fulfill({ status: 401, body: "" }));
});

test("precos_renderiza_3_planos_e_faq_abre", async ({ page }) => {
  await page.goto("/precos");
  await expect(page.getByTestId("plan-basico")).toBeVisible();
  await expect(page.getByTestId("plan-profissional")).toBeVisible();
  await expect(page.getByTestId("plan-enterprise")).toBeVisible();
  const q = page.getByRole("button", { name: "Existe fidelidade?" });
  await q.click();
  await expect(q).toHaveAttribute("aria-expanded", "true");
});
