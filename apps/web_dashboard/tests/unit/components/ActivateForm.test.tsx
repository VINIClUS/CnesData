import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { describe, expect, test, vi } from "vitest";

import { server } from "../../mocks/server";

import { ActivateForm } from "@/components/activate/ActivateForm";

vi.mock("@/auth/oidc", () => ({
  getAccessToken: vi.fn().mockResolvedValue("tok"),
}));

function wrap(ui: React.ReactNode) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(<QueryClientProvider client={qc}>{ui}</QueryClientProvider>);
}

const _TENANT = { ibge6: "354130", ibge7: "3541308", nome: "Presidente Epitácio", uf: "SP" };

describe("ActivateForm", () => {
  test("envia_user_code_e_mostra_sucesso", async () => {
    server.use(
      http.post("/activate/confirm", () =>
        HttpResponse.json({ status: "approved", expires_in_seconds: 300 }),
      ),
    );
    wrap(<ActivateForm tenants={[_TENANT]} />);
    await userEvent.type(screen.getByLabelText(/Código de ativação/i), "ABCD-EFGH");
    await userEvent.click(screen.getByRole("button", { name: /Aprovar/i }));
    await waitFor(() => {
      expect(screen.getByText(/Agente aprovado/i)).toBeInTheDocument();
    });
  });

  test("mostra_erro_quando_codigo_invalido", async () => {
    server.use(
      http.post("/activate/confirm", () =>
        HttpResponse.json({ detail: "invalid_or_expired_user_code" }, { status: 400 }),
      ),
    );
    wrap(<ActivateForm tenants={[_TENANT]} />);
    await userEvent.type(screen.getByLabelText(/Código de ativação/i), "EXPIRED1");
    await userEvent.click(screen.getByRole("button", { name: /Aprovar/i }));
    await waitFor(() => {
      expect(screen.getByText(/Código inválido ou expirado/i)).toBeInTheDocument();
    });
  });
});
