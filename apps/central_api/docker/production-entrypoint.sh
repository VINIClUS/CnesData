#!/bin/sh
set -eu

LEAF_CERTIFICATE=/etc/cnesdata/aws/leaf-certificate.pem
LEAF_PRIVATE_KEY=/etc/cnesdata/aws/leaf-private-key.pem

refuse() {
    printf '%s name=%s path=%s\n' "$1" "$2" "$3" >&2
    exit 2
}

# Any of these in the environment makes botocore pick method=env and never run the helper.
forbid_static_credential() {
    if [ -n "$2" ]; then
        printf 'static_credential_forbidden name=%s\n' "$1" >&2
        exit 2
    fi
}

require_file() {
    if [ ! -f "$2" ]; then
        refuse credential_file_missing "$1" "$2"
    fi
    if [ ! -r "$2" ]; then
        refuse credential_file_unreadable "$1" "$2"
    fi
}

# find prints the path only when others cannot read it; no output, or no find, refuses.
forbid_world_readable() {
    if [ -z "$(find -L "$2" ! -perm -0004)" ]; then
        refuse credential_file_insecure "$1" "$2"
    fi
}

forbid_static_credential AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID+set}"
forbid_static_credential AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY+set}"
forbid_static_credential AWS_SESSION_TOKEN "${AWS_SESSION_TOKEN+set}"
forbid_static_credential AWS_SECURITY_TOKEN "${AWS_SECURITY_TOKEN+set}"

require_file aws_config "${AWS_CONFIG_FILE:-}"
require_file leaf_certificate "$LEAF_CERTIFICATE"
require_file leaf_private_key "$LEAF_PRIVATE_KEY"
forbid_world_readable leaf_private_key "$LEAF_PRIVATE_KEY"

exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000
