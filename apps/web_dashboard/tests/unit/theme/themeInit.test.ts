import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

const _SCRIPT = readFileSync(resolve(__dirname, "../../../public/theme-init.js"), "utf8");
const _HTML = readFileSync(resolve(__dirname, "../../../index.html"), "utf8");

function runInit() {
  // The file ships as a classic browser script; executing its source is the point of the test.
  // eslint-disable-next-line @typescript-eslint/no-implied-eval, @typescript-eslint/no-unsafe-call
  new Function(_SCRIPT)();
}

function mockMatchMedia(matches: boolean) {
  window.matchMedia = ((query: string) => ({ matches, media: query })) as typeof window.matchMedia;
}

describe("theme-init.js", () => {
  beforeEach(() => {
    localStorage.clear();
    document.documentElement.classList.remove("dark");
    mockMatchMedia(false);
  });

  afterEach(() => {
    document.documentElement.classList.remove("dark");
  });

  test("aplica_dark_quando_preferencia_salva_e_dark", () => {
    localStorage.setItem("cnesdata-theme", "dark");
    runInit();
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  test("nao_aplica_dark_quando_preferencia_salva_e_light", () => {
    localStorage.setItem("cnesdata-theme", "light");
    mockMatchMedia(true);
    runInit();
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  test("system_segue_preferencia_do_sistema", () => {
    localStorage.setItem("cnesdata-theme", "system");
    mockMatchMedia(true);
    runInit();
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  test("sem_preferencia_salva_usa_system", () => {
    mockMatchMedia(true);
    runInit();
    expect(document.documentElement.classList.contains("dark")).toBe(true);
  });

  test("valor_invalido_cai_em_system", () => {
    localStorage.setItem("cnesdata-theme", "roxo");
    mockMatchMedia(false);
    runInit();
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  test("remove_dark_quando_preferencia_muda_para_light", () => {
    document.documentElement.classList.add("dark");
    localStorage.setItem("cnesdata-theme", "light");
    runInit();
    expect(document.documentElement.classList.contains("dark")).toBe(false);
  });

  test("nao_quebra_quando_localStorage_lanca", () => {
    const spy = vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => {
      throw new Error("storage bloqueado");
    });
    expect(() => runInit()).not.toThrow();
    expect(document.documentElement.classList.contains("dark")).toBe(false);
    spy.mockRestore();
  });
});

describe("index.html", () => {
  test("nao_contem_script_inline_bloqueado_pela_csp", () => {
    const inline = _HTML.match(/<script(?![^>]*\bsrc=)[^>]*>[\s\S]*?<\/script>/g) ?? [];
    expect(inline).toHaveLength(0);
  });

  test("carrega_theme_init_como_script_externo_no_head", () => {
    const head = _HTML.slice(_HTML.indexOf("<head>"), _HTML.indexOf("</head>"));
    expect(head).toContain('<script src="/theme-init.js"></script>');
    expect(head).not.toContain("defer");
    expect(head).not.toContain('type="module"');
  });
});
