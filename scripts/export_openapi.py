"""Exporta o schema OpenAPI da Central API para docs/openapi.json."""
import json
import sys
from pathlib import Path

from central_api.app import create_app

app = create_app()
schema = app.openapi()
output = Path("docs/openapi.json")
output.write_text(json.dumps(schema, indent=2, ensure_ascii=False))
sys.stdout.write(f"schema_exported path={output}\n")
