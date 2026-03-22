"""
test_evolution_tracker.py — Testes Unitários do Rastreador de Evolução (WP-006)

Cobertura:
  - criar_snapshot: conta corretamente cada categoria de anomalia.
  - salvar_snapshot / carregar_snapshots: round-trip JSON via tmp_path.
  - calcular_delta: deltas positivos, negativos e zero; tendência correta.
  - historico_completo: ordenação e quantidade de deltas gerados.
"""

import json
from pathlib import Path

import pandas as pd

from analysis.evolution_tracker import (
    Snapshot,
    Delta,
    criar_snapshot,
    salvar_snapshot,
    carregar_snapshots,
    calcular_delta,
    historico_completo,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _df(n: int) -> pd.DataFrame:
    """DataFrame com n linhas para simular contagem de anomalias."""
    return pd.DataFrame({"CPF": [str(i) * 11 for i in range(n)]})


def _snapshot(
    data: str = "2026-03",
    vinculos: int = 100,
    ghost: int = 5,
    missing: int = 3,
    rq005: int = 2,
) -> Snapshot:
    return Snapshot(
        data_competencia=data,
        total_vinculos=vinculos,
        total_ghost=ghost,
        total_missing=missing,
        total_rq005=rq005,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: criar_snapshot()
# ─────────────────────────────────────────────────────────────────────────────

class TestCriarSnapshot:

    def test_conta_vinculos_totais(self):
        s = criar_snapshot("2026-03", _df(367), _df(0), _df(0), _df(0), _df(0), _df(0))
        assert s.total_vinculos == 367

    def test_conta_ghost_totais(self):
        s = criar_snapshot("2026-03", _df(100), _df(8), _df(0), _df(0), _df(0), _df(0))
        assert s.total_ghost == 8

    def test_conta_missing_totais(self):
        s = criar_snapshot("2026-03", _df(100), _df(0), _df(4), _df(0), _df(0), _df(0))
        assert s.total_missing == 4

    def test_conta_rq005_como_soma_das_tres_regras(self):
        """total_rq005 = len(df_rq003b) + len(df_acs) + len(df_ace)."""
        s = criar_snapshot("2026-03", _df(100), _df(0), _df(0), _df(3), _df(5), _df(2))
        assert s.total_rq005 == 10

    def test_rq005_zero_quando_todos_vazios(self):
        s = criar_snapshot("2026-03", _df(100), _df(0), _df(0), _df(0), _df(0), _df(0))
        assert s.total_rq005 == 0

    def test_data_competencia_preservada(self):
        s = criar_snapshot("2025-11", _df(50), _df(0), _df(0), _df(0), _df(0), _df(0))
        assert s.data_competencia == "2025-11"

    def test_retorna_instancia_snapshot(self):
        s = criar_snapshot("2026-03", _df(10), _df(0), _df(0), _df(0), _df(0), _df(0))
        assert isinstance(s, Snapshot)


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: salvar_snapshot() / carregar_snapshots()
# ─────────────────────────────────────────────────────────────────────────────

class TestPersistencia:

    def test_salvar_cria_arquivo_json(self, tmp_path: Path):
        s = _snapshot("2026-03")
        caminho = salvar_snapshot(s, tmp_path)
        assert caminho.exists()
        assert caminho.suffix == ".json"

    def test_nome_arquivo_contem_data_competencia(self, tmp_path: Path):
        s = _snapshot("2026-03")
        caminho = salvar_snapshot(s, tmp_path)
        assert "2026-03" in caminho.name

    def test_salvar_cria_diretorio_se_inexistente(self, tmp_path: Path):
        diretorio = tmp_path / "subdir" / "snapshots"
        s = _snapshot("2026-03")
        salvar_snapshot(s, diretorio)
        assert diretorio.exists()

    def test_round_trip_preserva_todos_os_campos(self, tmp_path: Path):
        original = _snapshot("2026-03", vinculos=367, ghost=5, missing=3, rq005=7)
        salvar_snapshot(original, tmp_path)
        carregados = carregar_snapshots(tmp_path)

        assert len(carregados) == 1
        recuperado = carregados[0]
        assert recuperado.data_competencia == original.data_competencia
        assert recuperado.total_vinculos == original.total_vinculos
        assert recuperado.total_ghost == original.total_ghost
        assert recuperado.total_missing == original.total_missing
        assert recuperado.total_rq005 == original.total_rq005

    def test_carregar_retorna_lista_de_snapshots(self, tmp_path: Path):
        salvar_snapshot(_snapshot("2026-01"), tmp_path)
        salvar_snapshot(_snapshot("2026-02"), tmp_path)
        resultado = carregar_snapshots(tmp_path)
        assert isinstance(resultado, list)
        assert all(isinstance(s, Snapshot) for s in resultado)

    def test_carregar_diretorio_vazio_retorna_lista_vazia(self, tmp_path: Path):
        assert carregar_snapshots(tmp_path) == []

    def test_carregar_diretorio_inexistente_retorna_lista_vazia(self, tmp_path: Path):
        assert carregar_snapshots(tmp_path / "nao_existe") == []

    def test_carregar_ordenado_por_data_competencia(self, tmp_path: Path):
        salvar_snapshot(_snapshot("2026-03"), tmp_path)
        salvar_snapshot(_snapshot("2026-01"), tmp_path)
        salvar_snapshot(_snapshot("2026-02"), tmp_path)
        resultado = carregar_snapshots(tmp_path)
        datas = [s.data_competencia for s in resultado]
        assert datas == sorted(datas)

    def test_arquivo_json_valido(self, tmp_path: Path):
        s = _snapshot("2026-03")
        caminho = salvar_snapshot(s, tmp_path)
        dados = json.loads(caminho.read_text(encoding="utf-8"))
        assert "data_competencia" in dados
        assert "total_vinculos" in dados


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 3: calcular_delta()
# ─────────────────────────────────────────────────────────────────────────────

class TestCalcularDelta:

    def test_delta_ghost_positivo_quando_aumenta(self):
        anterior = _snapshot("2026-02", ghost=3)
        atual = _snapshot("2026-03", ghost=7)
        d = calcular_delta(anterior, atual)
        assert d.delta_ghost == 4

    def test_delta_ghost_negativo_quando_diminui(self):
        anterior = _snapshot("2026-02", ghost=10)
        atual = _snapshot("2026-03", ghost=4)
        d = calcular_delta(anterior, atual)
        assert d.delta_ghost == -6

    def test_delta_missing_calculado_corretamente(self):
        anterior = _snapshot("2026-02", missing=5)
        atual = _snapshot("2026-03", missing=2)
        d = calcular_delta(anterior, atual)
        assert d.delta_missing == -3

    def test_delta_rq005_calculado_corretamente(self):
        anterior = _snapshot("2026-02", rq005=8)
        atual = _snapshot("2026-03", rq005=8)
        d = calcular_delta(anterior, atual)
        assert d.delta_rq005 == 0

    def test_delta_vinculos_calculado_corretamente(self):
        anterior = _snapshot("2026-02", vinculos=350)
        atual = _snapshot("2026-03", vinculos=367)
        d = calcular_delta(anterior, atual)
        assert d.delta_vinculos == 17

    def test_data_anterior_e_atual_preservadas(self):
        anterior = _snapshot("2026-01")
        atual = _snapshot("2026-02")
        d = calcular_delta(anterior, atual)
        assert d.data_anterior == "2026-01"
        assert d.data_atual == "2026-02"

    def test_retorna_instancia_delta(self):
        d = calcular_delta(_snapshot("2026-01"), _snapshot("2026-02"))
        assert isinstance(d, Delta)

    def test_tendencia_melhora_quando_soma_negativa(self):
        """Soma(ghost+missing+rq005) < 0 → MELHORA."""
        anterior = _snapshot("2026-02", ghost=10, missing=5, rq005=4)
        atual = _snapshot("2026-03", ghost=6, missing=3, rq005=4)
        d = calcular_delta(anterior, atual)
        assert d.tendencia == "MELHORA"

    def test_tendencia_piora_quando_soma_positiva(self):
        """Soma(ghost+missing+rq005) > 0 → PIORA."""
        anterior = _snapshot("2026-02", ghost=2, missing=1, rq005=1)
        atual = _snapshot("2026-03", ghost=5, missing=3, rq005=2)
        d = calcular_delta(anterior, atual)
        assert d.tendencia == "PIORA"

    def test_tendencia_estavel_quando_soma_zero(self):
        """Soma(ghost+missing+rq005) == 0 → ESTAVEL."""
        s = _snapshot("2026-02", ghost=5, missing=3, rq005=2)
        d = calcular_delta(s, _snapshot("2026-03", ghost=5, missing=3, rq005=2))
        assert d.tendencia == "ESTAVEL"

    def test_tendencia_piora_basta_um_aumento(self):
        """Se ghost aumenta mas missing diminui no mesmo montante → pode ser PIORA."""
        anterior = _snapshot("2026-02", ghost=2, missing=4, rq005=0)
        atual = _snapshot("2026-03", ghost=5, missing=1, rq005=0)
        d = calcular_delta(anterior, atual)
        assert d.tendencia == "ESTAVEL"  # soma = +3 - 3 = 0


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 4: historico_completo()
# ─────────────────────────────────────────────────────────────────────────────

class TestHistoricoCompleto:

    def test_retorna_vazio_sem_snapshots(self, tmp_path: Path):
        assert historico_completo(tmp_path) == []

    def test_retorna_vazio_com_um_snapshot(self, tmp_path: Path):
        salvar_snapshot(_snapshot("2026-01"), tmp_path)
        assert historico_completo(tmp_path) == []

    def test_dois_snapshots_geram_um_delta(self, tmp_path: Path):
        salvar_snapshot(_snapshot("2026-01"), tmp_path)
        salvar_snapshot(_snapshot("2026-02"), tmp_path)
        resultado = historico_completo(tmp_path)
        assert len(resultado) == 1

    def test_tres_snapshots_geram_dois_deltas(self, tmp_path: Path):
        for mes in ["2026-01", "2026-02", "2026-03"]:
            salvar_snapshot(_snapshot(mes), tmp_path)
        resultado = historico_completo(tmp_path)
        assert len(resultado) == 2

    def test_deltas_em_ordem_cronologica(self, tmp_path: Path):
        salvar_snapshot(_snapshot("2026-01"), tmp_path)
        salvar_snapshot(_snapshot("2026-03"), tmp_path)
        salvar_snapshot(_snapshot("2026-02"), tmp_path)
        resultado = historico_completo(tmp_path)
        assert resultado[0].data_anterior == "2026-01"
        assert resultado[0].data_atual == "2026-02"

    def test_retorna_lista_de_delta(self, tmp_path: Path):
        salvar_snapshot(_snapshot("2026-01"), tmp_path)
        salvar_snapshot(_snapshot("2026-02"), tmp_path)
        resultado = historico_completo(tmp_path)
        assert all(isinstance(d, Delta) for d in resultado)
