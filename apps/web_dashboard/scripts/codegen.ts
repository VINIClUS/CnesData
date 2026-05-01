#!/usr/bin/env bun
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { resolve } from "node:path";

const _override = process.env.OPENAPI_PATH;
const openapi =
  _override ?? resolve(import.meta.dir, "..", "..", "..", "docs", "contracts", "openapi.json");
const out = resolve(import.meta.dir, "..", "src", "api", "generated.ts");

if (!existsSync(openapi)) {
  console.error(`OpenAPI spec not found: ${openapi}`);
  process.exit(1);
}

const openapiResult = spawnSync("bunx", ["openapi-typescript", openapi, "-o", out], {
  stdio: "inherit",
  shell: process.platform === "win32",
});

if (openapiResult.status !== 0) {
  process.exit(openapiResult.status ?? 1);
}

const routesResult = spawnSync("bunx", ["tsr", "generate"], {
  stdio: "inherit",
  shell: process.platform === "win32",
});

if (routesResult.status !== 0) {
  process.exit(routesResult.status ?? 1);
}
