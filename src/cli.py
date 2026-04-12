"""cli.py — Interface de linha de comando do pipeline CnesData."""

import argparse
from dataclasses import dataclass

__version__ = "1.0.0"

_ANO_MIN = 2000
_ANO_MAX = 2099


@dataclass(frozen=True)
class CliArgs:
    """Argumentos CLI parseados e validados."""

    competencia: tuple[int, int] | None
    output_dir: str | None
    source: str
    verbose: bool
    force_reingestao: bool = False


def parse_args(argv: list[str] | None = None) -> CliArgs:
    """Parseia argumentos CLI e retorna CliArgs validado.

    Args:
        argv: Lista de argumentos (None = sys.argv[1:]). Parametrizável para testes.

    Returns:
        CliArgs com valores parseados.

    Raises:
        SystemExit: Se --help, --version, ou argumento inválido.
    """
    parser = argparse.ArgumentParser(
        prog="cnesdata",
        description="Pipeline de auditoria CNES — Presidente Epitácio/SP",
    )
    parser.add_argument(
        "-c", "--competencia",
        type=str,
        default=None,
        metavar="YYYY-MM",
        help="Competência da base nacional (ex: 2024-12). Padrão: valor do .env.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        type=str,
        default=None,
        metavar="CAMINHO",
        help="Diretório de saída dos relatórios. Padrão: valor do .env.",
    )
    parser.add_argument(
        "--source",
        choices=["LOCAL", "NACIONAL", "AMBOS"],
        default="LOCAL",
        help="Fonte de dados: LOCAL (padrão), NACIONAL ou AMBOS.",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Ativa log DEBUG no console.",
    )
    parser.add_argument(
        "--force-reingestao",
        action="store_true",
        default=False,
        help="Força re-ingestão do Firebird mesmo quando snapshot local existir.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    args = parser.parse_args(argv)

    competencia = None
    if args.competencia is not None:
        try:
            competencia = _validar_competencia(args.competencia)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))

    return CliArgs(
        competencia=competencia,
        output_dir=args.output_dir,
        source=args.source,
        verbose=args.verbose,
        force_reingestao=args.force_reingestao,
    )


def _validar_competencia(valor: str) -> tuple[int, int]:
    try:
        partes = valor.split("-")
        if len(partes) != 2:
            raise ValueError
        ano, mes = int(partes[0]), int(partes[1])
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"competencia={valor} formato_esperado=YYYY-MM"
        )
    if ano < _ANO_MIN or ano > _ANO_MAX:
        raise argparse.ArgumentTypeError(
            f"competencia={valor} ano_fora_de_range={_ANO_MIN}-{_ANO_MAX}"
        )
    if mes < 1 or mes > 12:
        raise argparse.ArgumentTypeError(
            f"competencia={valor} mes_fora_de_range=1-12"
        )
    return (ano, mes)
