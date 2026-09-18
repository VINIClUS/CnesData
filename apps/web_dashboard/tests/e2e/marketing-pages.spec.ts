import { expect, test } from "@playwright/test";

const PUBLIC_PATHS = ["/", "/recursos", "/sobre", "/contato", "/login"];

test.beforeEach(async ({ context }) => {
  await context.route("**/api/v1/dashboard/auth/me", (r) => r.fulfill({ status: 401, body: "" }));
});

test("home_nao_divulga_precos", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator('a[href^="/precos"]')).toHaveCount(0);
});

test("recursos_tem_pagina_propria", async ({ page }) => {
  await page.goto("/recursos");
  await expect(
    page.getByRole("heading", { level: 1, name: "Conheça os recursos do CnesData" }),
  ).toBeVisible();
  await page.reload();
  await expect(page).toHaveURL(/\/recursos$/);
  await expect(page).toHaveTitle(/Recursos/);
});

test("sobre_tem_pagina_propria", async ({ page }) => {
  await page.goto("/sobre");
  await expect(
    page.getByRole("heading", { level: 1, name: "Por que o CnesData está sendo desenvolvido" }),
  ).toBeVisible();
  await page.reload();
  await expect(page).toHaveURL(/\/sobre$/);
});

test("contato_muda_titulo_conforme_interesse", async ({ page }) => {
  await page.goto("/contato");
  await expect(
    page.getByRole("heading", { level: 1, name: "Converse sobre o CnesData" }),
  ).toBeVisible();
  await page.goto("/contato?interesse=piloto");
  await expect(
    page.getByRole("heading", { level: 1, name: "Tenho interesse em participar de um piloto" }),
  ).toBeVisible();
  await page.goto("/contato?interesse=xyz");
  await expect(
    page.getByRole("heading", { level: 1, name: "Converse sobre o CnesData" }),
  ).toBeVisible();
});

test("navbar_marca_pagina_ativa_e_voltar_avancar_funciona", async ({ page }) => {
  await page.goto("/");
  const nav = page.getByRole("navigation", { name: "Principal", exact: true });
  await nav.getByRole("link", { name: "Sobre" }).click();
  await expect(page).toHaveURL(/\/sobre$/);
  await expect(nav.getByRole("link", { name: "Sobre" })).toHaveAttribute("aria-current", "page");
  await nav.getByRole("link", { name: "Recursos" }).click();
  await expect(page).toHaveURL(/\/recursos$/);
  await page.goBack();
  await expect(page).toHaveURL(/\/sobre$/);
  await page.goForward();
  await expect(page).toHaveURL(/\/recursos$/);
});

test("menu_mobile_abre_fecha_com_escape_e_navega", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  const button = page.getByRole("button", { name: "Menu", exact: true });
  await button.click();
  await expect(button).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(button).toHaveAttribute("aria-expanded", "false");
  await expect(button).toBeFocused();
  await button.click();
  await page
    .getByRole("navigation", { name: "Menu de navegação" })
    .getByRole("link", { name: "Sobre" })
    .click();
  await expect(page).toHaveURL(/\/sobre$/);
  await expect(page.getByRole("button", { name: "Menu", exact: true })).toHaveAttribute(
    "aria-expanded",
    "false",
  );
});

test("sem_overflow_horizontal_a_360px_nas_paginas_novas", async ({ page }) => {
  await page.setViewportSize({ width: 360, height: 740 });
  for (const path of ["/recursos", "/sobre", "/contato"]) {
    await page.goto(path);
    const [scroll, client] = await page.evaluate(() => [
      document.documentElement.scrollWidth,
      document.documentElement.clientWidth,
    ]);
    expect(scroll, path).toBeLessThanOrEqual(client);
  }
});

test("formulario_valida_obrigatorios_e_foca_primeiro_erro", async ({ page }) => {
  await page.goto("/contato");
  await page.getByRole("button", { name: "Enviar interesse" }).click();
  await expect(page.getByText("Informe seu nome.")).toBeVisible();
  await expect(page.getByRole("textbox", { name: "Nome" })).toBeFocused();
});

test("formulario_mostra_falha_real_com_retry_e_email", async ({ page }) => {
  await page.route("**/api/v1/public/leads", (r) =>
    r.fulfill({ status: 404, json: { detail: "Not Found" } }),
  );
  await page.goto("/contato?interesse=acesso-antecipado");
  await page.getByRole("textbox", { name: "Nome" }).fill("Pessoa de teste");
  await page.getByRole("textbox", { name: "E-mail" }).fill("pessoa@example.org");
  await page.getByRole("button", { name: "Enviar interesse" }).click();
  const status = page.getByRole("status");
  await expect(status).toContainText("Não foi possível registrar");
  await expect(status.getByRole("button", { name: "Tentar novamente" })).toBeVisible();
  await expect(status.getByRole("link", { name: "Enviar e-mail" })).toHaveAttribute(
    "href",
    /^mailto:me@vinisantana\.com/,
  );
  await expect(page.getByText("Recebemos seu interesse")).toHaveCount(0);
  await expect(page.getByRole("textbox", { name: "Nome" })).toHaveValue("Pessoa de teste");
});

test("formulario_recebido_apos_202", async ({ page }) => {
  await page.route("**/api/v1/public/leads", (r) =>
    r.fulfill({ status: 202, json: { status: "received" } }),
  );
  await page.goto("/contato");
  await page.getByRole("textbox", { name: "Nome" }).fill("Pessoa de teste");
  await page.getByRole("textbox", { name: "E-mail" }).fill("pessoa@example.org");
  await page.getByRole("button", { name: "Enviar interesse" }).click();
  await expect(page.getByRole("status")).toContainText("Recebemos seu interesse no CnesData");
});

test("precos_continua_acessivel_sem_sessao", async ({ page }) => {
  await page.goto("/precos");
  await expect(page.getByTestId("plan-basico")).toBeVisible();
  await expect(page.getByTestId("plan-profissional")).toBeVisible();
  await page.reload();
  await expect(page.getByTestId("plan-basico")).toBeVisible({ timeout: 15_000 });
  await expect(page.locator('head meta[name="robots"]')).toHaveAttribute("content", "noindex");
  await expect(page).toHaveTitle(/Planos e preços/);
});

test("noindex_de_precos_nao_vaza_para_home", async ({ page }) => {
  await page.goto("/precos");
  await page.getByRole("link", { name: "CnesData", exact: true }).click();
  await expect(page).toHaveURL(/\/$/);
  await expect(page.locator('meta[name="robots"][content*="noindex"]')).toHaveCount(0);
});

for (const path of PUBLIC_PATHS) {
  test(`nao_divulga_precos_em_${path}`, async ({ page }) => {
    await page.goto(path);
    await expect(page.locator('a[href^="/precos"]')).toHaveCount(0);
  });
}
