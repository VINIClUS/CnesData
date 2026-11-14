#!/bin/sh
set -eu

refuse() {
    printf '%s name=%s path=%s\n' "$1" "$2" "$3" >&2
    exit 2
}

# Any of these in the environment makes botocore pick method=env before the shared file.
forbid_static_credential() {
    if [ -n "$2" ]; then
        printf 'static_credential_forbidden name=%s\n' "$1" >&2
        exit 2
    fi
}

# Each of these selects a profile, config or provider other than the host-written [default].
forbid_credential_source() {
    if [ -n "$2" ]; then
        printf 'credential_source_forbidden name=%s\n' "$1" >&2
        exit 2
    fi
}

require_setting() {
    if [ "$2" != "$3" ]; then
        printf 'setting_mismatch name=%s\n' "$1" >&2
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

# The host gate writes exactly [default] with a temporary (ASIA) key and its session token.
require_temporary_profile() {
    count=0
    while IFS= read -r line; do
        count=$((count + 1))
        case "$count:$line" in
            "1:[default]" | 2:aws_access_key_id=ASIA?* | 3:aws_secret_access_key=?*) ;;
            4:aws_session_token=?*) ;;
            *) refuse credential_file_mismatch "$1" "$2" ;;
        esac
    done < "$2"
    if [ "$count" -ne 4 ]; then
        refuse credential_file_mismatch "$1" "$2"
    fi
}

forbid_static_credential AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID+set}"
forbid_static_credential AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY+set}"
forbid_static_credential AWS_SESSION_TOKEN "${AWS_SESSION_TOKEN+set}"
forbid_static_credential AWS_SECURITY_TOKEN "${AWS_SECURITY_TOKEN+set}"
forbid_credential_source AWS_WEB_IDENTITY_TOKEN_FILE "${AWS_WEB_IDENTITY_TOKEN_FILE+set}"
forbid_credential_source AWS_ROLE_ARN "${AWS_ROLE_ARN+set}"
forbid_credential_source AWS_DEFAULT_PROFILE "${AWS_DEFAULT_PROFILE+set}"
forbid_credential_source AWS_PROFILE "${AWS_PROFILE+set}"
forbid_credential_source AWS_CONFIG_FILE "${AWS_CONFIG_FILE+set}"
require_setting PROFILE "${PROFILE:-}" aws
require_setting AWS_PRODUCTION_POLICY "${AWS_PRODUCTION_POLICY:-}" true
require_setting AWS_SHARED_CREDENTIALS_FILE "${AWS_SHARED_CREDENTIALS_FILE:-}" \
    /run/credentials/credentials

require_file aws_credentials "${AWS_SHARED_CREDENTIALS_FILE:-}"
forbid_world_readable aws_credentials "${AWS_SHARED_CREDENTIALS_FILE:-}"
require_temporary_profile aws_credentials "${AWS_SHARED_CREDENTIALS_FILE:-}"

exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000
