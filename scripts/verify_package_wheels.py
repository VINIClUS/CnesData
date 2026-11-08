"""Valida wheels instalados sem carregar editable paths do checkout."""

import logging
import subprocess
import sys
import sysconfig
from pathlib import Path
from zipfile import ZipFile

_PROBE = """
from importlib import import_module, resources
from pathlib import Path
import sys

installed = Path(sys.argv[1]).resolve()
sys.path[:0] = [str(installed), sys.argv[2]]
names = sys.argv[3:]
for name in names:
    module = import_module(name)
    assert Path(module.__file__).is_relative_to(installed), name
for name in (
    'cnes_data.storage.schema_v2', 'cnes_data.dynamodb',
    'cnes_identity.oidc', 'cnes_identity.dynamodb_certificates',
    'cnes_identity.dynamodb_refresh_tokens', 'cnes_identity.sqlite_certificates',
    'cnes_data.public_leads.dynamodb', 'cnes_identity.certificates',
    'cnes_identity.refresh_contracts', 'cnes_governance',
    'cnes_control_plane.persistence', 'cnes_control_plane.billing',
    'cnes_runtime.aws', 'cnes_observability.query_count',
):
    module = import_module(name)
    assert Path(module.__file__).is_relative_to(installed), name
from alembic.config import Config
from alembic.script import ScriptDirectory
config = Config()
config.set_main_option('script_location', 'cnes_data:alembic')
scripts = ScriptDirectory.from_config(config)
assert len(scripts.get_heads()) == 1
assert len(list(scripts.walk_revisions())) == 20
assert resources.files('cnes_data').joinpath('alembic/script.py.mako').is_file()
assert all(not name.startswith('cnes_infra') for name in sys.modules)
"""


def main() -> None:
    artifact_dir = Path(sys.argv[1]).resolve()
    names = []
    for wheel in sorted((artifact_dir / "wheels").glob("*.whl")):
        name = wheel.name.split("-")[0]
        names.append(name)
        with ZipFile(wheel) as archive:
            paths = [Path(path) for path in archive.namelist()]
            assert Path(name, "__init__.py") in paths, wheel.name
            assert all("tests" not in path.parts for path in paths), wheel.name
            assert all(path.name not in {"CLAUDE.md", "pyproject.toml"} for path in paths)
            source = Path(__file__).resolve().parents[1] / "packages" / name
            expected = {
                Path(name) / path.relative_to(source)
                for path in source.rglob("*.py")
                if "tests" not in path.relative_to(source).parts
            }
            assert expected <= set(paths), (wheel.name, expected - set(paths))
    assert len(names) == 8, names
    result = subprocess.run(
        [sys.executable, "-I", "-S", "-c", _PROBE, str(artifact_dir / "installed"),
         sysconfig.get_path("purelib"), *names],
        cwd=artifact_dir,
        check=True,
    )
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__).info(
        "wheel_artifacts_valid packages=%s exit_code=%d", names, result.returncode,
    )


if __name__ == "__main__":
    main()
