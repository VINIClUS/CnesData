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

# Each of these makes botocore resolve credentials before, or instead of, the helper.
forbid_credential_source() {
    if [ -n "$2" ]; then
        printf 'credential_source_forbidden name=%s\n' "$1" >&2
        exit 2
    fi
}

require_setting() {
    if [ "$2" != "$3" ]; then
        printf 'credential_setting_mismatch name=%s\n' "$1" >&2
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
forbid_credential_source AWS_WEB_IDENTITY_TOKEN_FILE "${AWS_WEB_IDENTITY_TOKEN_FILE+set}"
forbid_credential_source AWS_ROLE_ARN "${AWS_ROLE_ARN+set}"
forbid_credential_source AWS_DEFAULT_PROFILE "${AWS_DEFAULT_PROFILE+set}"
require_setting AWS_PROFILE "${AWS_PROFILE:-}" cnesdata-production
require_setting AWS_CONFIG_FILE "${AWS_CONFIG_FILE:-}" /etc/cnesdata/aws/config
require_setting AWS_SHARED_CREDENTIALS_FILE "${AWS_SHARED_CREDENTIALS_FILE:-}" /dev/null

require_file aws_config "${AWS_CONFIG_FILE:-}"
require_file leaf_certificate "$LEAF_CERTIFICATE"
require_file leaf_private_key "$LEAF_PRIVATE_KEY"
forbid_world_readable leaf_private_key "$LEAF_PRIVATE_KEY"

exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000
