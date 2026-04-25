import { http, HttpResponse } from "msw";

export const handlers = [
  http.get("/api/v1/dashboard/auth/me", () =>
    HttpResponse.json({
      user_id: "00000000-0000-0000-0000-000000000001",
      email: "g@m",
      display_name: "Gestor",
      role: "gestor",
      tenant_ids: ["354130"],
    }),
  ),
];
