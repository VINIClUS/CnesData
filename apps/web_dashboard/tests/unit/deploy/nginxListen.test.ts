import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "vitest";

const DOCKERFILE = readFileSync(resolve(__dirname, "../../../Dockerfile"), "utf8");
const NGINX_TEMPLATE = readFileSync(
  resolve(__dirname, "../../../nginx/default.conf.template"),
  "utf8",
);

test("nginx template escuta em IPv4 e IPv6", () => {
  expect(NGINX_TEMPLATE).toContain("listen 80;");
  expect(NGINX_TEMPLATE).toContain("listen [::]:80;");
});

test("Dockerfile healthcheck usa 127.0.0.1, não localhost", () => {
  const healthcheckLine = DOCKERFILE.split("\n").find((line) => line.includes("HEALTHCHECK"));
  expect(healthcheckLine).toContain("http://127.0.0.1/healthz");
  expect(healthcheckLine).not.toContain("localhost");
});
