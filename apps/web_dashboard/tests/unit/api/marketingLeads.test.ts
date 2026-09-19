import { HttpResponse, http } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { submitLead } from "@/api/marketingLeads";
import type { LeadPayload } from "@/api/marketingLeads";

vi.mock("@/auth/oidc", () => ({ getAccessToken: vi.fn().mockResolvedValue(null) }));

const payload: LeadPayload = {
  name: "Pessoa",
  email: "pessoa@example.org",
  interest: "early_access",
  organization: "",
  role: "",
  message: "",
  municipality: "",
  source_path: "/contato",
  source_cta: "acesso-antecipado",
  privacy_notice_version: "1",
  newsletter_opt_in: false,
};

describe("submitLead", () => {
  test("retorna_ok_em_202_e_envia_payload_sem_bearer", async () => {
    let received: unknown;
    let auth: string | null = "x";
    server.use(
      http.post("/api/v1/public/leads", async ({ request }) => {
        received = await request.json();
        auth = request.headers.get("Authorization");
        return HttpResponse.json({ status: "received" }, { status: 202 });
      }),
    );
    await expect(submitLead(payload)).resolves.toEqual({ ok: true });
    expect(received).toEqual(payload);
    expect(auth).toBeNull();
  });

  test.each([
    [422, "validation"],
    [429, "rate_limited"],
    [404, "unavailable"],
    [503, "unavailable"],
    [500, "unavailable"],
  ])("mapeia_status_%i_para_%s", async (status, reason) => {
    server.use(
      http.post("/api/v1/public/leads", () => HttpResponse.json({ detail: "x" }, { status })),
    );
    await expect(submitLead(payload)).resolves.toEqual({ ok: false, reason });
  });

  test("mapeia_erro_de_rede", async () => {
    server.use(http.post("/api/v1/public/leads", () => HttpResponse.error()));
    await expect(submitLead(payload)).resolves.toEqual({ ok: false, reason: "network" });
  });
});
