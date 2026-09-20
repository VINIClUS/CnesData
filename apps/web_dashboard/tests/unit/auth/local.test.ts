import { http, HttpResponse } from "msw";
import { describe, expect, test } from "vitest";

import { server } from "../../mocks/server";

import { getLocalPrincipal, logoutLocal } from "@/auth/local";

describe("sessao local", () => {
  test("get_local_principal_usa_rota_auth_me_registrada", async () => {
    const principal = {
      user_id: "user-1",
      email: "gestor@example.com",
      tenant_id: "354130",
      role: "gestor" as const,
    };
    server.use(http.get("/api/v1/auth/me", () => HttpResponse.json(principal)));

    await expect(getLocalPrincipal()).resolves.toEqual(principal);
  });

  test("logout_local_aceita_resposta_sem_corpo", async () => {
    server.use(http.post("/api/v1/auth/logout", () => new HttpResponse(null, { status: 204 })));

    await expect(logoutLocal()).resolves.toBeNull();
  });
});
