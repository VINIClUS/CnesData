"""cli.py — Interface de linha de comando do dump_agent."""

import argparse
from dataclasses import dataclass

__version__ = "2.0.0"

_ANO_MIN = 2000
_ANO_MAX = 2099


@dataclass(frozen=True)
class CliArgs:
    """Argumentos CLI parseados e validados."""

    competencia: tuple[int, int] | None
    source: str
    verbose: bool
    api_url: str
    machine_id: str


def parse_args(argv: list[str] | None = None) -> CliArgs:
    """Parseia argumentos CLI e retorna CliArgs validado."""
    parser = argparse.ArgumentParser(
        prog="dump-agent",
        description="Agente de extração CNES — streaming via pre-signed URL",
    )
    parser.add_argument(
        "-c", "--competencia",
        type=str,
        default=None,
        metavar="YYYY-MM",
        help="Competência (ex: 2024-12). Padrão: .env.",
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
        "--api-url",
        type=str,
        default="http://localhost:8000",
        help="URL base da central_api.",
    )
    parser.add_argument(
        "--machine-id",
        type=str,
        default=None,
        help="Identificador da máquina agente.",
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
        source=args.source,
        verbose=args.verbose,
        api_url=args.api_url,
        machine_id=args.machine_id or "",
    )


def _validar_competencia(valor: str) -> tuple[int, int]:
    try:
        partes = valor.split("-")
        if len(partes) != 2:
            raise ValueError
        ano, mes = int(partes[0]), int(partes[1])
    except ValueError as err:
        raise argparse.ArgumentTypeError(
            f"competencia={valor} formato_esperado=YYYY-MM"
        ) from err
    if ano < _ANO_MIN or ano > _ANO_MAX:
        raise argparse.ArgumentTypeError(
            f"competencia={valor} ano_fora_de_range={_ANO_MIN}-{_ANO_MAX}"
        )
    if mes < 1 or mes > 12:
        raise argparse.ArgumentTypeError(
            f"competencia={valor} mes_fora_de_range=1-12"
        )
    return (ano, mes)
