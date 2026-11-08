import { expect } from "@playwright/test";
import type { Route } from "@playwright/test";

import { ACCESS_TOKEN } from "./oidc";

type AuthenticatedResponse = {
  json: unknown;
  tenantId?: string;
  status?: number;
  headers?: Record<string, string>;
};

export async function authenticatedJson(route: Route, options: AuthenticatedResponse) {
  const request = route.request();
  expect(request.headers().authorization).toBe(`Bearer ${ACCESS_TOKEN}`);
  expect(request.headers()["x-tenant-id"]).toBe(options.tenantId);
  const { tenantId: _tenantId, ...response } = options;
  await route.fulfill(response);
}
