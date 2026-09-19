import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "vitest";

const TEMPLATE = readFileSync(
  resolve(__dirname, "../../../nginx/default.conf.template"),
  "utf8",
);

test("preserva o protocolo HTTPS recebido do proxy externo", () => {
  expect(TEMPLATE).toContain(
    "proxy_set_header   X-Forwarded-Proto $http_x_forwarded_proto;",
  );
  expect(TEMPLATE).not.toContain("proxy_set_header   X-Forwarded-Proto $scheme;");
});
