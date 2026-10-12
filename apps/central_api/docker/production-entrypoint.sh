#!/bin/sh
set -eu

LEAF_CERTIFICATE=/etc/cnesdata/aws/leaf-certificate.pem
LEAF_PRIVATE_KEY=/etc/cnesdata/aws/leaf-private-key.pem

require_file() {
    if [ ! -f "$2" ] || [ ! -r "$2" ]; then
        printf 'credential_file_missing name=%s path=%s\n' "$1" "$2" >&2
        exit 2
    fi
}

require_file aws_config "${AWS_CONFIG_FILE:-}"
require_file leaf_certificate "$LEAF_CERTIFICATE"
require_file leaf_private_key "$LEAF_PRIVATE_KEY"

exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000
