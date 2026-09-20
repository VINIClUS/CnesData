import { RouterProvider, createMemoryHistory, createRouter } from "@tanstack/react-router";
import { render, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, test } from "vitest";

import { server } from "../../mocks/server";

import { AuthProvider } from "@/auth/AuthProvider";
import { env } from "@/lib/env";
import { routeTree } from "@/routeTree.gen";
import { ThemeProvider } from "@/theme/ThemeProvider";

describe("guardião das rotas autenticadas", () => {
  test("sessao_local_ausente_redireciona_para_login", async () => {
    env.VITE_AUTH_MODE = "local";
    server.use(
      http.get("/api/v1/auth/local/me", () =>
        HttpResponse.json({ detail: "not authenticated" }, { status: 401 }),
      ),
    );
    const router = createRouter({
      routeTree,
      history: createMemoryHistory({ initialEntries: ["/overview"] }),
    });

    render(
      <ThemeProvider>
        <AuthProvider>
          <RouterProvider router={router} />
        </AuthProvider>
      </ThemeProvider>,
    );

    await waitFor(() => expect(router.state.location.pathname).toBe("/login"));
  });
});
