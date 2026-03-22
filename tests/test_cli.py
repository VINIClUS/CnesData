"""test_cli.py — Testes unitários do parser CLI."""

import pytest

from cli import CliArgs, _validar_competencia, parse_args


class TestParsingArgumentos:

    def test_sem_argumentos_retorna_defaults(self):
        args = parse_args([])
        assert args.competencia is None
        assert args.skip_nacional is False
        assert args.skip_hr is False
        assert args.verbose is False
        assert args.output_dir is None

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

    def test_output_dir_string(self):
        args = parse_args(["-o", "/tmp/saida"])
        assert args.output_dir == "/tmp/saida"

    def test_skip_nacional_flag(self):
        args = parse_args(["--skip-nacional"])
        assert args.skip_nacional is True

    def test_skip_hr_flag(self):
        args = parse_args(["--skip-hr"])
        assert args.skip_hr is True

    def test_verbose_flag_curto(self):
        args = parse_args(["-v"])
        assert args.verbose is True

    def test_verbose_flag_longo(self):
        args = parse_args(["--verbose"])
        assert args.verbose is True

    def test_version_levanta_system_exit(self):
        with pytest.raises(SystemExit):
            parse_args(["--version"])

    def test_combinacao_de_flags(self):
        args = parse_args(["-c", "2024-12", "--skip-nacional", "-v"])
        assert args.competencia == (2024, 12)
        assert args.skip_nacional is True
        assert args.verbose is True


class TestDataclassCliArgs:

    def test_cli_args_e_frozen(self):
        args = CliArgs(
            competencia=None,
            output_dir=None,
            skip_nacional=False,
            skip_hr=False,
            verbose=False,
        )
        with pytest.raises(Exception):
            args.verbose = True  # type: ignore[misc]

    def test_cli_args_campos_corretos(self):
        args = CliArgs(
            competencia=(2024, 12),
            output_dir="/tmp/out",
            skip_nacional=True,
            skip_hr=True,
            verbose=True,
        )
        assert args.competencia == (2024, 12)
        assert args.output_dir == "/tmp/out"
        assert args.skip_nacional is True
        assert args.skip_hr is True
        assert args.verbose is True


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
