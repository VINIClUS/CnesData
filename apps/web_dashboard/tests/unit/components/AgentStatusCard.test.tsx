import type { SourceStatus } from "@/api/hooks/useAgentStatus";
import { AgentStatusCard } from "@/components/agentes/AgentStatusCard";
import { render, screen } from "@testing-library/react";
import { describe, expect, test } from "vitest";

const _OK: SourceStatus = {
  fonte_sistema: "CNES_LOCAL",
  last_extracao_ts: new Date(Date.now() - 6 * 60 * 60 * 1000).toISOString(),
  last_competencia: 202604,
  lag_months: 0,
  row_count: 4521,
  status: "ok",
  last_machine_id: "PE-EDGE-01",
};

describe("AgentStatusCard", () => {
  test("renderiza_status_ok_com_competencia_e_row_count", () => {
    render(<AgentStatusCard data={_OK} />);
    expect(screen.getByText(/CNES_LOCAL/)).toBeInTheDocument();
    expect(screen.getByText(/abr\/2026/i)).toBeInTheDocument();
    expect(screen.getByText(/4\.521 linhas/)).toBeInTheDocument();
    expect(screen.getByText(/em dia/i)).toBeInTheDocument();
    expect(screen.getByText(/PE-EDGE-01/)).toBeInTheDocument();
  });

  test("renderiza_status_no_data_quando_sem_extracao", () => {
    render(
      <AgentStatusCard
        data={{
          ..._OK,
          status: "no_data",
          last_extracao_ts: null,
          last_competencia: null,
          lag_months: null,
          row_count: null,
          last_machine_id: null,
        }}
      />,
    );
    expect(screen.getByText(/sem extração/i)).toBeInTheDocument();
  });
});
