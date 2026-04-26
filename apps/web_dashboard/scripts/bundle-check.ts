#!/usr/bin/env bun
import { gzipSync } from "node:zlib";
import { readFileSync, readdirSync } from "node:fs";
import { join, dirname, resolve } from "node:path";

const _MAIN_BUDGET = 200 * 1024;
const _ROUTE_BUDGET = 100 * 1024;
const _DIST = resolve(dirname(import.meta.dir), "dist", "assets");

function gzipSize(p: string): number {
  return gzipSync(readFileSync(p)).length;
}

function listJs(): string[] {
  return readdirSync(_DIST)
    .filter((f) => f.endsWith(".js"))
    .map((f) => join(_DIST, f));
}

const files = listJs();
let failed = 0;
for (const f of files) {
  const size = gzipSize(f);
  const isMain = /index-[^/]+\.js$/.test(f) || /main-[^/]+\.js$/.test(f);
  const budget = isMain ? _MAIN_BUDGET : _ROUTE_BUDGET;
  const status = size <= budget ? "OK" : "FAIL";
  if (status === "FAIL") failed += 1;
  const sizeKb = (size / 1024).toFixed(1);
  const budgetKb = (budget / 1024).toFixed(0);
  console.log(`${status} ${f} ${sizeKb}KB (budget ${budgetKb}KB)`);
}
if (failed > 0) {
  console.error(`bundle:check failed: ${failed} chunks over budget`);
  process.exit(1);
}
