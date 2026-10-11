import { expect, test } from "@playwright/test";

const SERVING_PATH = "/api/v1/dashboard/serving/cnes/overview";
const LEGACY_PATHS = ["/api/v1/dashboard/overview", "/api/v1/dashboard/faturamento"];

const V1 = {
  schema_version: "cnes-serving-v1",
  tenant_id: "354130",
  run_id: "run-v1",
  generated_at: "2026-01-31T23:59:59Z",
  competencia: "2026-01",
  kpis: {
    match_count: 3,
    local_only_count: 2,
    national_only_count: 2,
    conflict_count: 1,
    reconciled_row_count: 7,
    active_professional_count: 6,
  },
  divergence_counts: { NOME_PROFISSIONAL: 1 } as Record<string, number>,
  missing_sources: [] as string[],
};

const V2 = {
  schema_version: "cnes-serving-v1",
  tenant_id: "354130",
  run_id: "run-v2",
  generated_at: "2026-02-28T23:59:59Z",
  competencia: "2026-02",
  kpis: {
    match_count: 11,
    local_only_count: 0,
    national_only_count: 5,
    conflict_count: 4,
    reconciled_row_count: 20,
    active_professional_count: 9,
  },
  divergence_counts: { CNS: 4 } as Record<string, number>,
  missing_sources: ["CNES_NACIONAL"],
};

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

test("troca_atomica_do_pointer_renderiza_somente_a_nova_versao_sem_rotas_legadas", async ({
  context,
  page,
}) => {
  const pointer = { current: V1 };
  const requested: string[] = [];
  context.on("request", (request) => {
    requested.push(new URL(request.url()).pathname);
  });
  await context.route(`**${SERVING_PATH}`, (r) =>
    r.fulfill({
      json: pointer.current,
      headers: { "X-Dataset-Version": pointer.current.run_id },
    }),
  );

  await page.goto("/overview");
  await expect(page.getByText("Correspondências 01/2026")).toBeVisible();
  await expect(page.getByText("NOME_PROFISSIONAL: 1")).toBeVisible();

  pointer.current = V2;
  const reloaded = page.waitForResponse((response) => response.url().endsWith(SERVING_PATH));
  await page.reload();

  expect((await reloaded).headers()["x-dataset-version"]).toBe("run-v2");
  await expect(page.getByText("Correspondências 02/2026")).toBeVisible();
  await expect(page.getByText("CNS: 4")).toBeVisible();
  await expect(page.getByText("CNES_NACIONAL")).toBeVisible();
  await expect(page.getByText("Correspondências 01/2026")).toHaveCount(0);
  await expect(page.getByText("NOME_PROFISSIONAL")).toHaveCount(0);

  const apiCalls = requested.filter((path) => path.startsWith("/api/"));
  expect(apiCalls.filter((path) => LEGACY_PATHS.some((legacy) => path.startsWith(legacy)))).toEqual(
    [],
  );
  expect(apiCalls.filter((path) => path.startsWith("/api/v1/dashboard/serving/"))).toEqual([
    SERVING_PATH,
    SERVING_PATH,
  ]);
});
