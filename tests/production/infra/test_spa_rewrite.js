"use strict";

const test = require("node:test");
const assert = require("node:assert/strict");
const vm = require("node:vm");
const fs = require("node:fs");
const path = require("node:path");
const { createHash } = require("node:crypto");

const sourcePath = path.resolve(__dirname,
  "../../../infra/opentofu/modules/static-web/function.js");
const cases = [
  ["J01_raiz_sem_query", "/", "/index.html", "absent"],
  ["J02_rota_aninhada", "/dashboard/tenant", "/index.html", "empty"],
  ["J03_prefixo_api_delimitado", "/apiary", "/index.html", "empty"],
  ["J04_prefixo_assets_delimitado", "/assets-old", "/index.html", "empty"],
  ["J05_api_raiz", "/api", "/api", "empty"],
  ["J06_api_com_barra", "/api/", "/api/", "empty"],
  ["J07_api_filha", "/api/v1/x", "/api/v1/x", "empty"],
  ["J08_assets_raiz", "/assets", "/assets", "empty"],
  ["J09_asset_sem_extensao", "/assets/sem-extensao", "/assets/sem-extensao", "empty"],
  ["J10_metadata_raiz", "/.well-known", "/.well-known", "empty"],
  ["J11_metadata_filha", "/.well-known/openid-configuration",
    "/.well-known/openid-configuration", "empty"],
  ["J12_asset_hash", "/assets/app.ab12cd34.js", "/assets/app.ab12cd34.js", "empty"],
  ["J13_favicon", "/favicon.ico", "/favicon.ico", "empty"],
  ["J14_manifest", "/manifest.webmanifest", "/manifest.webmanifest", "empty"],
  ["J15_service_worker", "/service-worker.js", "/service-worker.js", "empty"],
  ["J16_robots", "/robots.txt", "/robots.txt", "empty"],
  ["J17_arquivo_fora_assets", "/downloads/report.csv", "/downloads/report.csv", "empty"],
  ["J18_ponto_em_segmento_intermediario", "/v1.2/path", "/v1.2/path", "empty"],
  ["J19_rota_com_barra_final", "/dashboard/tenant/", "/index.html", "empty"],
  ["J20_query_multipla_preservada", "/dashboard", "/index.html", "multiple"],
];
const controls = [
  ["K01_reescreve_api", "J05_api_raiz", 'result.uri = "/index.html"; return result;'],
  ["K02_reescreve_asset", "J12_asset_hash", 'result.uri = "/index.html"; return result;'],
  ["K03_reescreve_metadata", "J11_metadata_filha",
    'result.uri = "/index.html"; return result;'],
  ["K04_reescreve_segmento_com_ponto", "J18_ponto_em_segmento_intermediario",
    'result.uri = "/index.html"; return result;'],
  ["K05_clona_query", "J20_query_multipla_preservada",
    'result.querystring = JSON.parse(JSON.stringify(result.querystring)); return result;'],
  ["K06_clona_request", "J02_rota_aninhada", "return { ...result };"],
  ["K07_altera_outro_campo", "J02_rota_aninhada",
    'result.headers["x-canary"].value = "alterado"; return result;'],
];

function readSource() {
  assert.ok(fs.existsSync(sourcePath), `arquivo_ausente caminho=${sourcePath}`);
  assert.ok(fs.statSync(sourcePath).isFile(), `arquivo_invalido caminho=${sourcePath}`);
  return fs.readFileSync(sourcePath, "utf8");
}

function digest(source) {
  return createHash("sha256").update(source).digest("hex");
}

function requestFor(row) {
  const request = {
    uri: row[1], method: "GET", headers: { "x-canary": { value: "preservado" } },
    cookies: { session: { value: "synthetic", attributes: "Secure; HttpOnly" } },
    canary: { nested: ["original", { value: 17 }] },
  };
  if (row[3] !== "absent") request.querystring = {};
  if (row[3] === "multiple") {
    request.querystring = {
      q: { value: "a%2Fb+%25", multiValue: [{ value: "a%2Fb+%25" }, { value: "%E2%82%AC" }] },
      empty: { value: "" },
    };
  }
  return request;
}

function invoke(source, request) {
  const context = vm.createContext({ event: { request } }, {
    codeGeneration: { strings: false, wasm: false },
  });
  new vm.Script('"use strict";\n' + source, { filename: sourcePath })
    .runInContext(context, { timeout: 1000 });
  assert.equal(vm.runInContext("typeof handler", context, { timeout: 1000 }), "function",
    "handler_ausente");
  assert.equal(vm.runInContext(
    "[typeof process,typeof require,typeof fetch,typeof setTimeout,typeof setInterval].join(',')",
    context, { timeout: 1000 }), "undefined,undefined,undefined,undefined,undefined");
  return new vm.Script("handler(event)").runInContext(context, { timeout: 1000 });
}

function assertCase(source, row) {
  const request = requestFor(row);
  const references = {
    query: request.querystring, headers: request.headers, cookies: request.cookies,
  };
  const keys = Reflect.ownKeys(request);
  const expected = structuredClone(request);
  expected.uri = row[2];
  const result = invoke(source, request);
  assert.strictEqual(result, request, `request_identidade caso=${row[0]}`);
  assert.strictEqual(request.querystring, references.query, `query_identidade caso=${row[0]}`);
  assert.strictEqual(request.headers, references.headers, `headers_identidade caso=${row[0]}`);
  assert.strictEqual(request.cookies, references.cookies, `cookies_identidade caso=${row[0]}`);
  assert.deepStrictEqual(Reflect.ownKeys(request), keys, `chaves caso=${row[0]}`);
  assert.deepStrictEqual(request, expected, `request_valor caso=${row[0]}`);
}

for (const row of cases) {
  test(`test_reescrita_${row[0]}`, () => assertCase(readSource(), row));
}

for (const [id, target, mutation] of controls) {
  test(`test_controle_${id}`, () => {
    const original = readSource();
    const before = digest(original);
    const row = cases.find((entry) => entry[0] === target);
    assert.ok(row, `controle_caso_ausente id=${id}`);
    try {
      assertCase(original, row);
      const corrupt = original + "\nconst originalHandler = handler;\n" +
        "handler = function(event) { const result = originalHandler(event);\n" +
        mutation + "\n};\n";
      assert.notEqual(digest(corrupt), before, `mutacao_sem_delta id=${id}`);
      assert.throws(() => assertCase(corrupt, row),
        { name: "AssertionError", code: "ERR_ASSERTION" });
    } finally {
      assert.equal(digest(readSource()), before, `source_modificado caminho=${sourcePath}`);
    }
  });
}
