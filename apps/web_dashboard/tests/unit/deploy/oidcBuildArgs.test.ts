import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { expect, test } from "vitest";

const DOCKERFILE = readFileSync(resolve(__dirname, "../../../Dockerfile"), "utf8");
const DEPLOY_DEVELOP = readFileSync(
  resolve(__dirname, "../../../../../.github/workflows/deploy-develop.yml"),
  "utf8",
);
const DEPLOY_MAIN = readFileSync(
  resolve(__dirname, "../../../../../.github/workflows/deploy-main.yml"),
  "utf8",
);

const OIDC_VARS = ["VITE_OIDC_AUTHORITY", "VITE_OIDC_CLIENT_ID", "VITE_OIDC_REDIRECT_URI"];

test("Dockerfile declara ARG e ENV para as três variáveis OIDC do build", () => {
  for (const name of OIDC_VARS) {
    expect(DOCKERFILE).toContain(`ARG ${name}`);
    expect(DOCKERFILE).toContain(`ENV ${name}=$${name}`);
  }
});

test("deploy-develop.yml passa build-args OIDC para o web_dashboard", () => {
  for (const name of OIDC_VARS) {
    expect(DEPLOY_DEVELOP).toContain(`${name}=`);
  }
});

test("deploy-main.yml passa build-args OIDC para o web_dashboard", () => {
  for (const name of OIDC_VARS) {
    expect(DEPLOY_MAIN).toContain(`${name}=`);
  }
});
