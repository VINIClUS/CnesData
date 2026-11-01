"""Leitura do arquivo privado de input do transporte CLI dentro de fronteiras falsas."""

import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

PRIVATE_INPUT = "file://<private-input>"


def capture_input(
    argv: Sequence[str], options: Mapping[str, Any]
) -> tuple[list[str], dict[str, Any]]:
    arguments = list(argv)
    if "--cli-input-json" not in arguments:
        return arguments, dict(options)
    index = arguments.index("--cli-input-json") + 1
    location = arguments[index]
    assert location.startswith("file:///"), "transport=private_file_required"
    assert location != "file:///dev/stdin", "transport=stdin_forbidden"
    assert options.get("stdin") == subprocess.DEVNULL, "transport=stdin_devnull_required"
    assert "input" not in options, "transport=stdin_payload_forbidden"
    text = Path(location.removeprefix("file://")).read_text(encoding="utf-8")
    recorded = {key: value for key, value in options.items() if key != "stdin"}
    return [*arguments[:index], PRIVATE_INPUT, *arguments[index + 1 :]], {
        **recorded,
        "input": text,
    }
