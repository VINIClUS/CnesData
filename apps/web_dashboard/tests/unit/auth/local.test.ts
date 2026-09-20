import { http, HttpResponse } from "msw";
import { describe, expect, test } from "vitest";

import { server } from "../../mocks/server";

import { logoutLocal } from "@/auth/local";

describe("sessao local", () => {
  test("logout_local_aceita_resposta_sem_corpo", async () => {
    server.use(http.post("/api/v1/auth/logout", () => new HttpResponse(null, { status: 204 })));

    await expect(logoutLocal()).resolves.toBeNull();
  });
});
