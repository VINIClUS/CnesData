"""Testes do DatabaseLoader — persistência DuckDB Gold layer."""

from pathlib import Path

import duckdb
import pandas as pd

from storage.database_loader import DatabaseLoader
from storage.historico_reader import HistoricoReader
from analysis.evolution_tracker import Snapshot


def _tabelas_existentes(caminho_db: Path) -> list[str]:
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SHOW ALL TABLES").df()
    con.close()
    return df["name"].tolist()


class TestInicializarSchema:
    def test_cria_tabela_evolucao_metricas(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act
        loader.inicializar_schema()

        # Assert
        assert "evolucao_metricas_mensais" in _tabelas_existentes(
            tmp_path / "test.duckdb"
        )

    def test_cria_tabela_auditoria_resultados(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act
        loader.inicializar_schema()

        # Assert
        assert "auditoria_resultados" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_idempotente_chamada_multiplas_vezes(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")

        # Act — não deve levantar exceção
        loader.inicializar_schema()
        loader.inicializar_schema()


def _snapshot(competencia: str = "2024-12", vinculos: int = 357) -> Snapshot:
    return Snapshot(
        data_competencia=competencia,
        total_vinculos=vinculos,
        total_ghost=5,
        total_missing=3,
        total_rq005=8,
    )


def _ler_metricas(caminho_db: Path):
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SELECT * FROM gold.evolucao_metricas_mensais").df()
    con.close()
    return df


class TestGravarMetricas:
    def test_insere_snapshot_na_tabela_gold(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_metricas(_snapshot())

        # Assert
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["data_competencia"].iloc[0] == "2024-12"
        assert df["total_vinculos"].iloc[0] == 357

    def test_upsert_substitui_competencia_existente(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot(vinculos=100))

        # Act — mesma competência, vinculos diferentes
        loader.gravar_metricas(_snapshot(vinculos=357))

        # Assert — deve haver apenas 1 linha (UPSERT, não INSERT duplo)
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["total_vinculos"].iloc[0] == 357

    def test_multiplas_competencias_inseridas(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_metricas(_snapshot("2024-11"))
        loader.gravar_metricas(_snapshot("2024-12"))

        # Assert
        df = _ler_metricas(tmp_path / "test.duckdb")
        assert len(df) == 2
        assert set(df["data_competencia"]) == {"2024-11", "2024-12"}


def _ler_auditoria(caminho_db: Path):
    con = duckdb.connect(str(caminho_db), read_only=True)
    df = con.execute("SELECT * FROM gold.auditoria_resultados ORDER BY regra").df()
    con.close()
    return df


class TestGravarAuditoria:
    def test_insere_contagem_por_regra(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_auditoria("2024-12", "RQ006", 3)

        # Assert
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["regra"].iloc[0] == "RQ006"
        assert df["total_anomalias"].iloc[0] == 3

    def test_upsert_atualiza_contagem_existente(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_auditoria("2024-12", "RQ006", 3)

        # Act — reexecução com nova contagem
        loader.gravar_auditoria("2024-12", "RQ006", 5)

        # Assert — linha única com valor atualizado
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 1
        assert df["total_anomalias"].iloc[0] == 5

    def test_insere_multiplas_regras_mesma_competencia(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        loader.gravar_auditoria("2024-12", "RQ006", 2)
        loader.gravar_auditoria("2024-12", "RQ008", 7)

        # Assert
        df = _ler_auditoria(tmp_path / "test.duckdb")
        assert len(df) == 2
        assert set(df["regra"]) == {"RQ006", "RQ008"}


class TestCarregarHistorico:
    def test_retorna_lista_vazia_quando_banco_sem_dados(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()

        # Act
        resultado = loader.carregar_historico()

        # Assert
        assert resultado == []

    def test_retorna_snapshots_em_ordem_cronologica(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot("2024-11", vinculos=300))
        loader.gravar_metricas(_snapshot("2024-12", vinculos=357))

        # Act
        resultado = loader.carregar_historico()

        # Assert
        assert len(resultado) == 2
        assert resultado[0].data_competencia == "2024-11"
        assert resultado[1].data_competencia == "2024-12"
        assert resultado[1].total_vinculos == 357

    def test_snapshot_retornado_tem_todos_os_campos(self, tmp_path):
        # Arrange
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas(_snapshot("2024-12"))

        # Act
        resultado = loader.carregar_historico()

        # Assert
        s = resultado[0]
        assert s.data_competencia == "2024-12"
        assert s.total_vinculos == 357
        assert s.total_ghost == 5
        assert s.total_missing == 3
        assert s.total_rq005 == 8


class TestNovasTabelas:
    def test_cria_glosas_profissional(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "glosas_profissional" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_cache_nacional(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "cache_nacional" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_metricas_avancadas(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "metricas_avancadas" in _tabelas_existentes(tmp_path / "test.duckdb")


class TestGravarGlosas:
    def test_insere_glosas(self, tmp_path):
        import pandas as pd
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = pd.DataFrame([{
            "competencia": "2026-03", "regra": "GHOST",
            "cpf": "12345678901", "cns": None,
            "nome_profissional": "JOAO", "sexo": "M",
            "cnes_estabelecimento": "1234567", "motivo": "AUSENTE_NO_RH",
            "criado_em_firebird": None,
            "criado_em_pipeline": datetime(2026, 3, 31),
            "atualizado_em_pipeline": datetime(2026, 3, 31),
        }])
        loader.gravar_glosas("2026-03", "GHOST", df)
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            resultado = con.execute(
                "SELECT * FROM gold.glosas_profissional WHERE competencia='2026-03'"
            ).df()
        assert len(resultado) == 1
        assert resultado.iloc[0]["cpf"] == "12345678901"

    def test_delete_antes_de_inserir(self, tmp_path):
        import pandas as pd
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        def _row(cpf):
            return pd.DataFrame([{
                "competencia": "2026-03", "regra": "GHOST",
                "cpf": cpf, "cns": None, "nome_profissional": "X",
                "sexo": None, "cnes_estabelecimento": None, "motivo": None,
                "criado_em_firebird": None,
                "criado_em_pipeline": datetime(2026, 3, 31),
                "atualizado_em_pipeline": datetime(2026, 3, 31),
            }])
        loader.gravar_glosas("2026-03", "GHOST", _row("11111111111"))
        loader.gravar_glosas("2026-03", "GHOST", _row("22222222222"))
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            total = con.execute(
                "SELECT COUNT(*) FROM gold.glosas_profissional WHERE competencia='2026-03' AND regra='GHOST'"
            ).fetchone()[0]
        assert total == 1


class TestCacheNacional:
    def test_gravar_e_ler_cache(self, tmp_path):
        from datetime import datetime
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_cache_nacional("2026-03", "abc123")
        resultado = loader.ler_cache_nacional("2026-03")
        assert resultado is not None
        fingerprint, gravado_em = resultado
        assert fingerprint == "abc123"
        assert isinstance(gravado_em, datetime)

    def test_ler_cache_retorna_none_se_ausente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert loader.ler_cache_nacional("2026-03") is None

    def test_gravar_cache_sobrescreve(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_cache_nacional("2026-03", "primeiro")
        loader.gravar_cache_nacional("2026-03", "segundo")
        fp, _ = loader.ler_cache_nacional("2026-03")
        assert fp == "segundo"


class TestMetricasAvancadas:
    def test_gravar_metricas(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_metricas_avancadas("2026-03", {
            "taxa_anomalia_geral": 0.12,
            "p90_ch_total": 40.0,
            "proporcao_feminina_geral": 0.65,
            "n_reincidentes": 3,
            "taxa_resolucao": 0.5,
            "velocidade_regularizacao_media": 2.0,
            "top_glosas_json": "[]",
            "anomalias_por_cbo_json": "[]",
            "proporcao_feminina_por_cnes_json": "[]",
            "ranking_cnes_json": "[]",
        })
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT taxa_anomalia_geral FROM gold.metricas_avancadas WHERE competencia='2026-03'"
            ).df()
        assert abs(df.iloc[0]["taxa_anomalia_geral"] - 0.12) < 1e-6


class TestDeltaLocalSnapshot:
    def test_cria_tabela_delta_local_snapshot(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "delta_local_snapshot" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_gravar_e_ler_delta(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        delta = {
            "n_novos": 3,
            "n_removidos": 1,
            "n_alterados": 2,
            "novos_json": '[{"CPF": "00000000001", "CNES": "1111111"}]',
            "removidos_json": "[]",
            "alterados_json": "[]",
        }
        loader.gravar_delta_snapshot("2026-03", delta)

        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        resultado = reader.carregar_delta_snapshot("2026-03")

        assert resultado is not None
        assert resultado["n_novos"] == 3
        assert resultado["n_removidos"] == 1
        assert resultado["n_alterados"] == 2

    def test_gravar_delta_upsert_idempotente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        delta_v1 = {"n_novos": 1, "n_removidos": 0, "n_alterados": 0,
                    "novos_json": "[]", "removidos_json": "[]", "alterados_json": "[]"}
        delta_v2 = {"n_novos": 5, "n_removidos": 2, "n_alterados": 1,
                    "novos_json": "[]", "removidos_json": "[]", "alterados_json": "[]"}
        loader.gravar_delta_snapshot("2026-03", delta_v1)
        loader.gravar_delta_snapshot("2026-03", delta_v2)

        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        resultado = reader.carregar_delta_snapshot("2026-03")
        assert resultado["n_novos"] == 5

    def test_carregar_delta_retorna_none_quando_ausente(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        reader = HistoricoReader(tmp_path / "test.duckdb", tmp_path / "historico")
        assert reader.carregar_delta_snapshot("2026-03") is None


def _df_prof_sample() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS":              ["123456789012345"],
        "CPF":              ["12345678901"],
        "NOME_PROFISSIONAL":["Ana Silva"],
        "SEXO":             ["F"],
        "CBO":              ["515105"],
        "CNES":             ["2795001"],
        "TIPO_VINCULO":     ["30"],
        "SUS":              ["S"],
        "CH_TOTAL":         [40],
        "CH_AMBULATORIAL":  [20],
        "CH_OUTRAS":        [10],
        "CH_HOSPITALAR":    [10],
        "FONTE":            ["LOCAL"],
        "ALERTA_STATUS_CH": ["OK"],
        "DESCRICAO_CBO":    ["Agente Comunitário"],
    })


def _df_estab_sample() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES":             ["2795001"],
        "NOME_FANTASIA":    ["UBS Centro"],
        "TIPO_UNIDADE":     ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA":["1023"],
        "COD_MUNICIPIO":    ["354130"],
        "VINCULO_SUS":      ["S"],
        "FONTE":            ["LOCAL"],
    })


class TestNovasTabelasDDL:
    def test_cria_profissionais_processados(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "profissionais_processados" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_estabelecimentos(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "estabelecimentos" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_cbo_lookup(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "cbo_lookup" in _tabelas_existentes(tmp_path / "test.duckdb")

    def test_cria_pipeline_runs(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert "pipeline_runs" in _tabelas_existentes(tmp_path / "test.duckdb")


class TestGravarCarregarProfissionais:
    def test_profissional_existe_falso_quando_vazio(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert not loader.profissional_existe("2026-03")

    def test_profissional_existe_verdadeiro_apos_gravar(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_profissionais("2026-03", _df_prof_sample())
        assert loader.profissional_existe("2026-03")

    def test_roundtrip_profissionais(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = _df_prof_sample()
        loader.gravar_profissionais("2026-03", df)
        resultado = loader.carregar_profissionais("2026-03")
        assert list(resultado["CPF"]) == ["12345678901"]
        assert list(resultado["CNES"]) == ["2795001"]

    def test_gravar_profissionais_substitui_existentes(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_profissionais("2026-03", _df_prof_sample())
        df2 = _df_prof_sample().copy()
        df2["CPF"] = ["99999999999"]
        df2["CNES"] = ["9999999"]
        loader.gravar_profissionais("2026-03", df2)
        resultado = loader.carregar_profissionais("2026-03")
        assert len(resultado) == 1
        assert resultado["CPF"].iloc[0] == "99999999999"


class TestGravarCarregarEstabelecimentos:
    def test_roundtrip_estabelecimentos(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        df = _df_estab_sample()
        loader.gravar_estabelecimentos("2026-03", df)
        resultado = loader.carregar_estabelecimentos("2026-03")
        assert list(resultado["CNES"]) == ["2795001"]
        assert list(resultado["NOME_FANTASIA"]) == ["UBS Centro"]


class TestGravarCarregarCboLookup:
    def test_roundtrip_cbo_lookup(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        lookup = {"515105": "Agente Comunitário", "225125": "Médico"}
        loader.gravar_cbo_lookup("2026-03", lookup)
        resultado = loader.carregar_cbo_lookup("2026-03")
        assert resultado == lookup

    def test_lookup_vazio_retorna_dict_vazio(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        assert loader.carregar_cbo_lookup("2026-03") == {}


class TestGravarPipelineRun:
    def test_gravar_pipeline_run_completo(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_pipeline_run("2026-03", True, True, False, "completo")
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT * FROM gold.pipeline_runs WHERE competencia='2026-03'"
            ).df()
        assert len(df) == 1
        assert df["status"].iloc[0] == "completo"
        assert df["local_disponivel"].iloc[0] == True  # noqa: E712 — np.True_ vs True

    def test_gravar_pipeline_run_upsert(self, tmp_path):
        loader = DatabaseLoader(tmp_path / "test.duckdb")
        loader.inicializar_schema()
        loader.gravar_pipeline_run("2026-03", False, True, False, "sem_dados_locais")
        loader.gravar_pipeline_run("2026-03", True, True, False, "completo")
        with duckdb.connect(str(tmp_path / "test.duckdb"), read_only=True) as con:
            df = con.execute(
                "SELECT status FROM gold.pipeline_runs WHERE competencia='2026-03'"
            ).df()
        assert len(df) == 1
        assert df["status"].iloc[0] == "completo"
