#!/bin/sh
set -eu

# Any of these in the environment makes botocore pick method=env before the image profile.
forbid_static_credential() {
    if [ -n "$2" ]; then
        printf 'static_credential_forbidden name=%s\n' "$1" >&2
        exit 2
    fi
}

# Each of these selects a profile, file or provider other than the image credential_process.
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

forbid_static_credential AWS_ACCESS_KEY_ID "${AWS_ACCESS_KEY_ID+set}"
forbid_static_credential AWS_SECRET_ACCESS_KEY "${AWS_SECRET_ACCESS_KEY+set}"
forbid_static_credential AWS_SESSION_TOKEN "${AWS_SESSION_TOKEN+set}"
forbid_static_credential AWS_SECURITY_TOKEN "${AWS_SECURITY_TOKEN+set}"
forbid_credential_source AWS_WEB_IDENTITY_TOKEN_FILE "${AWS_WEB_IDENTITY_TOKEN_FILE+set}"
forbid_credential_source AWS_ROLE_ARN "${AWS_ROLE_ARN+set}"
forbid_credential_source AWS_DEFAULT_PROFILE "${AWS_DEFAULT_PROFILE+set}"
forbid_credential_source AWS_PROFILE "${AWS_PROFILE+set}"
forbid_credential_source AWS_CONTAINER_CREDENTIALS_RELATIVE_URI \
    "${AWS_CONTAINER_CREDENTIALS_RELATIVE_URI+set}"
forbid_credential_source AWS_CONTAINER_CREDENTIALS_FULL_URI \
    "${AWS_CONTAINER_CREDENTIALS_FULL_URI+set}"
forbid_credential_source AWS_CONTAINER_AUTHORIZATION_TOKEN \
    "${AWS_CONTAINER_AUTHORIZATION_TOKEN+set}"
forbid_credential_source AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE \
    "${AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE+set}"
forbid_credential_source AWS_CREDENTIAL_FILE "${AWS_CREDENTIAL_FILE+set}"
forbid_credential_source BOTO_CONFIG "${BOTO_CONFIG+set}"
require_setting PROFILE "${PROFILE:-}" aws
require_setting AWS_PRODUCTION_POLICY "${AWS_PRODUCTION_POLICY:-}" true
require_setting AWS_CONFIG_FILE "${AWS_CONFIG_FILE:-}" /etc/cnesdata/aws-config
require_setting AWS_SHARED_CREDENTIALS_FILE "${AWS_SHARED_CREDENTIALS_FILE:-}" /dev/null
require_setting AWS_EC2_METADATA_DISABLED "${AWS_EC2_METADATA_DISABLED:-}" true

# The reader prints the credential on stdout; the preflight keeps only its key=value refusal.
/usr/local/bin/cnesdata-aws-credentials /run/credentials/aws-credentials.json >/dev/null

exec /usr/local/bin/uvicorn central_api.app:create_app --factory --host 0.0.0.0 --port 8000
