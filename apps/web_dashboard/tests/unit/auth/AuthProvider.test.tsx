import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { AuthProvider } from "@/auth/AuthProvider";
import { getAccessToken } from "@/auth/oidc";
import { useAuth } from "@/auth/useAuth";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok-1"),
}));

const mockGetAccessToken = vi.mocked(getAccessToken);

beforeEach(() => {
  mockGetAccessToken.mockResolvedValue("tok-1");
});

function Probe() {
  const { user, status } = useAuth();
  return (
    <div data-testid="probe">
      {status}|{user?.email ?? "anon"}
    </div>
  );
}

describe("AuthProvider", () => {
  test("estado_inicial_eh_loading", () => {
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    expect(screen.getByTestId("probe").textContent).toBe("loading|anon");
  });

  test("popula_user_quando_session_valida", async () => {
    server.use(
      http.get("/api/v1/dashboard/auth/me", () =>
        HttpResponse.json({
          user_id: "u-1",
          email: "g@m",
          display_name: "G",
          role: "gestor",
          tenant_ids: ["354130"],
        }),
      ),
    );
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("probe").textContent).toBe("authenticated|g@m");
    });
  });

  test("retorna_anonymous_quando_me_responde_401", async () => {
    server.use(
      http.get("/api/v1/dashboard/auth/me", () =>
        HttpResponse.json({ detail: "auth_required" }, { status: 401 }),
      ),
    );
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("probe").textContent).toBe("anonymous|anon");
    });
  });

  test("anexa_bearer_no_fetch_de_me", async () => {
    let captured: string | null = "not-captured";
    server.use(
      http.get("/api/v1/dashboard/auth/me", ({ request }) => {
        captured = request.headers.get("authorization");
        return HttpResponse.json({
          user_id: "u-1",
          email: "g@m",
          display_name: "G",
          role: "gestor",
          tenant_ids: ["354130"],
        });
      }),
    );
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(captured).toBe("Bearer tok-1");
    });
  });

  test("retorna_anonymous_sem_chamar_me_quando_sem_token", async () => {
    mockGetAccessToken.mockResolvedValueOnce(null);
    let called = false;
    server.use(
      http.get("/api/v1/dashboard/auth/me", () => {
        called = true;
        return HttpResponse.json({});
      }),
    );
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await waitFor(() => {
      expect(screen.getByTestId("probe").textContent).toBe("anonymous|anon");
    });
    expect(called).toBe(false);
  });

  test("retorna_anonymous_quando_fetch_lanca_erro", async () => {
    server.use(http.get("/api/v1/dashboard/auth/me", () => HttpResponse.error()));
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await waitFor(() => expect(screen.getByTestId("probe").textContent).toBe("anonymous|anon"));
  });
});
