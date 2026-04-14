"""test_cli.py — Testes unitários do parser CLI."""

import pytest

from dump_agent.cli import CliArgs, _validar_competencia, parse_args


class TestParsingArgumentos:

    def test_sem_argumentos_retorna_defaults(self):
        args = parse_args([])
        assert args.competencia is None
        assert args.source == "LOCAL"
        assert args.verbose is False
        assert args.api_url == "http://localhost:8000"
        assert args.machine_id == ""

    def test_competencia_valida_2024_12(self):
        args = parse_args(["-c", "2024-12"])
        assert args.competencia == (2024, 12)

    def test_competencia_valida_2025_01(self):
        args = parse_args(["--competencia", "2025-01"])
        assert args.competencia == (2025, 1)

    def test_competencia_formato_invalido_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["-c", "2024/12"])

    def test_competencia_mes_13_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["-c", "2024-13"])

    def test_competencia_mes_0_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["-c", "2024-00"])

    def test_competencia_ano_1999_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["-c", "1999-01"])

    def test_competencia_texto_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["-c", "abc-de"])

    def test_source_flag_nacional(self):
        args = parse_args(["--source", "NACIONAL"])
        assert args.source == "NACIONAL"

    def test_source_flag_ambos(self):
        args = parse_args(["--source", "AMBOS"])
        assert args.source == "AMBOS"

    def test_source_invalido_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["--source", "INVALIDO"])

    def test_verbose_flag_curto(self):
        args = parse_args(["-v"])
        assert args.verbose is True

    def test_verbose_flag_longo(self):
        args = parse_args(["--verbose"])
        assert args.verbose is True

    def test_version_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["--version"])

    def test_api_url_custom(self):
        args = parse_args(["--api-url", "http://api:9000"])
        assert args.api_url == "http://api:9000"

    def test_machine_id_custom(self):
        args = parse_args(["--machine-id", "agent-01"])
        assert args.machine_id == "agent-01"

    def test_combinacao_de_flags(self):
        args = parse_args([
            "-c", "2024-12",
            "--source", "NACIONAL",
            "-v",
            "--api-url", "http://api:8080",
        ])
        assert args.competencia == (2024, 12)
        assert args.source == "NACIONAL"
        assert args.verbose is True
        assert args.api_url == "http://api:8080"


class TestDataclassCliArgs:

    def test_cli_args_e_frozen(self):
        args = CliArgs(
            competencia=None,
            source="LOCAL",
            verbose=False,
            api_url="http://localhost:8000",
            machine_id="test",
        )
        with pytest.raises(Exception):
            args.verbose = True  # type: ignore[misc]

    def test_cli_args_campos_corretos(self):
        args = CliArgs(
            competencia=(2024, 12),
            source="NACIONAL",
            verbose=True,
            api_url="http://api:9000",
            machine_id="agent-01",
        )
        assert args.competencia == (2024, 12)
        assert args.source == "NACIONAL"
        assert args.verbose is True
        assert args.api_url == "http://api:9000"
        assert args.machine_id == "agent-01"


class TestValidarCompetencia:

    def test_validar_competencia_limites_aceitos(self):
        assert _validar_competencia("2000-01") == (2000, 1)
        assert _validar_competencia("2099-12") == (2099, 12)

    def test_validar_competencia_limites_rejeitados(self):
        import argparse as _argparse
        with pytest.raises(_argparse.ArgumentTypeError):
            _validar_competencia("1999-12")
        with pytest.raises(_argparse.ArgumentTypeError):
            _validar_competencia("2100-01")
