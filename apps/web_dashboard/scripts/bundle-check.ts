#!/usr/bin/env bun
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { gzipSync } from "node:zlib";

const _MAIN_BUDGET = 200 * 1024;
const _ROUTE_BUDGET = 100 * 1024;
const _TREMOR_BUDGET = 100 * 1024;
const _DIST = resolve(dirname(import.meta.dir), "dist", "assets");

function gzipSize(p: string): number {
  return gzipSync(readFileSync(p)).length;
}

type Bucket = { name: string; budget: number };

function classify(file: string): Bucket {
  const base = file.toLowerCase();
  if (base.includes("tremor-")) return { name: "tremor", budget: _TREMOR_BUDGET };
  if (base.includes("index-")) return { name: "main", budget: _MAIN_BUDGET };
  return { name: "route", budget: _ROUTE_BUDGET };
}

const files = readdirSync(_DIST)
  .filter((f) => f.endsWith(".js"))
  .map((f) => join(_DIST, f));

let failed = 0;
for (const f of files) {
  const size = gzipSize(f);
  const { name, budget } = classify(f);
  const status = size <= budget ? "OK" : "FAIL";
  if (status === "FAIL") failed += 1;
  const sizeKb = (size / 1024).toFixed(1);
  const budgetKb = (budget / 1024).toFixed(0);
  console.log(`${status} ${name} ${f} ${sizeKb}KB (budget ${budgetKb}KB)`);
}

if (failed > 0) {
  console.error(`bundle:check failed: ${failed} chunks over budget`);
  process.exit(1);
}
