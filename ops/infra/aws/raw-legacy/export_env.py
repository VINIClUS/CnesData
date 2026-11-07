"""Exporta credenciais raw do estado Terraform para arquivos privados por ambiente."""

import json
import os
import sys
from pathlib import Path

state = json.loads(Path(sys.argv[1]).read_text())
destination = Path(sys.argv[2])
destination.mkdir(mode=0o700, parents=True, exist_ok=True)
ids = state["outputs"]["raw_access_key_ids"]["value"]
secrets = state["outputs"]["raw_secret_access_keys"]["value"]

for environment in ("dev", "prod"):
    path = destination / f"{environment}.env.raw"
    values = {
        "RAW_DYNAMODB_TABLE": f"cnesdata-raw-{environment}",
        "RAW_S3_BUCKET": f"cnesdata-raw-{environment}-836651842853",
        "RAW_AWS_REGION": "sa-east-1",
        "RAW_AWS_ACCESS_KEY_ID": ids[environment],
        "RAW_AWS_SECRET_ACCESS_KEY": secrets[environment],
    }
    content = "".join(f"{key}={value}\n" for key, value in values.items())
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as file:
        file.write(content)
