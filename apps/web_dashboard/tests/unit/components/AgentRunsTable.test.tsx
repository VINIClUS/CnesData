import type { RunRow } from "@/api/hooks/useAgentRuns";
import { AgentRunsTable } from "@/components/agentes/AgentRunsTable";
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

const _RUN: RunRow = {
  id: "abc",
  extracao_ts: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
  fonte_sistema: "CNES_LOCAL",
  competencia: 202604,
  row_count: 4521,
  sha256: "a".repeat(64),
  machine_id: null,
};

describe("AgentRunsTable", () => {
  test("renderiza_runs_com_formato_pt_br", () => {
    render(<AgentRunsTable runs={[_RUN]} />);
    expect(screen.getByText(/CNES_LOCAL/)).toBeInTheDocument();
    expect(screen.getByText(/abr\/2026/i)).toBeInTheDocument();
    expect(screen.getByText(/4\.521/)).toBeInTheDocument();
  });

  test("mostra_mensagem_vazia_sem_runs", () => {
    render(<AgentRunsTable runs={[]} />);
    expect(screen.getByText(/Sem execuções recentes/i)).toBeInTheDocument();
  });
});
