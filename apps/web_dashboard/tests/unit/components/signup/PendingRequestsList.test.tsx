import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

import type { AccessRequest } from "@/api/hooks/useAccessRequests";
import { PendingRequestsList } from "@/components/signup/PendingRequestsList";

const _PENDING: AccessRequest = {
  id: "abc",
  tenant_id: "354130",
  tenant_nome: "Presidente Epitácio",
  motivation: "x",
  status: "pending",
  requested_at: new Date(Date.now() - 60 * 1000).toISOString(),
  reviewed_at: null,
  review_notes: null,
};

describe("PendingRequestsList", () => {
  test("mostra_mensagem_vazia_sem_requests", () => {
    render(<PendingRequestsList requests={[]} />);
    expect(screen.getByText(/Nenhuma solicitação/i)).toBeInTheDocument();
  });

  test("renderiza_request_pendente_com_badge", () => {
    render(<PendingRequestsList requests={[_PENDING]} />);
    expect(screen.getByText("Presidente Epitácio")).toBeInTheDocument();
    expect(screen.getByText("Pendente")).toBeInTheDocument();
  });
});
