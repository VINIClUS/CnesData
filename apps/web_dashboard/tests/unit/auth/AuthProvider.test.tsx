import { render, screen, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test } from "vitest";

import { server } from "../../mocks/server";

import { AuthProvider } from "@/auth/AuthProvider";
import { useAuth } from "@/auth/useAuth";

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
});
