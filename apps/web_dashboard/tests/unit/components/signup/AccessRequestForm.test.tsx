import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../../mocks/server";

import { AccessRequestForm } from "@/components/signup/AccessRequestForm";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

function wrap(ui: React.ReactNode) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>);
}

describe("AccessRequestForm", () => {
  test("submete_e_mostra_sucesso", async () => {
    server.use(
      http.get("/api/v1/dashboard/access-requests/available-tenants", () =>
        HttpResponse.json([{ ibge6: "354130", ibge7: "3541308", nome: "PE", uf: "SP" }]),
      ),
      http.post("/api/v1/dashboard/access-requests", () =>
        HttpResponse.json({ request_id: "req-1" }, { status: 201 }),
      ),
    );
    wrap(<AccessRequestForm />);
    await userEvent.click(await screen.findByRole("combobox"));
    await userEvent.click(await screen.findByRole("option", { name: /PE \/ SP/ }));
    await userEvent.type(screen.getByLabelText(/Justificativa/i), "Sou gestor da SMS");
    await userEvent.click(screen.getByRole("button", { name: /Solicitar acesso/i }));
    await waitFor(() => {
      expect(screen.getByText(/Solicitação enviada/i)).toBeInTheDocument();
    });
  });

  test("mostra_erro_409_duplicate", async () => {
    server.use(
      http.get("/api/v1/dashboard/access-requests/available-tenants", () =>
        HttpResponse.json([{ ibge6: "354130", ibge7: "3541308", nome: "PE", uf: "SP" }]),
      ),
      http.post("/api/v1/dashboard/access-requests", () =>
        HttpResponse.json({ detail: "duplicate_request" }, { status: 409 }),
      ),
    );
    wrap(<AccessRequestForm />);
    await userEvent.click(await screen.findByRole("combobox"));
    await userEvent.click(await screen.findByRole("option", { name: /PE \/ SP/ }));
    await userEvent.type(screen.getByLabelText(/Justificativa/i), "x");
    await userEvent.click(screen.getByRole("button", { name: /Solicitar/i }));
    await waitFor(() => {
      expect(screen.getByText(/Você já tem solicitação/i)).toBeInTheDocument();
    });
  });
});
