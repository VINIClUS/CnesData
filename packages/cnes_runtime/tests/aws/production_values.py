"""Ambiente aws válido sob a política de produção, compartilhado entre suites."""

from types import MappingProxyType

PRODUCTION_SUBNET_IDS = ("subnet-00000000000000001", "subnet-00000000000000002")
PRODUCTION_SECURITY_GROUP_ID = "sg-00000000000000001"
PRODUCTION_STATE_MACHINE_ARN = (
    "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-prod"
)
PRODUCTION_OIDC_DOMAIN = "https://cnesdata-prod-example.auth.us-east-2.amazoncognito.com"
PRODUCTION_VALUES = MappingProxyType({
    "PROFILE": "aws",
    "AUTH_MODE": "oidc",
    "AWS_PRODUCTION_POLICY": "true",
    "AWS_ENVIRONMENT": "prod",
    "AWS_REGION": "us-east-2",
    "AWS_CONTROL_PLANE_TABLE": "cnesdata-prod-control-plane",
    "AWS_DATA_BUCKET": "cnesdata-prod-data",
    "AWS_AUDIT_BUCKET": "cnesdata-prod-audit",
    "AWS_STATE_MACHINE_ARN": PRODUCTION_STATE_MACHINE_ARN,
    "AWS_PROCESSOR_CONTAINER_NAME": "processor",
    "AWS_PROCESSOR_MAX_CONCURRENCY": "1",
    "AWS_PROCESSOR_LEASE_SECONDS": "3600",
    "AWS_PROCESSOR_RECOVERY_BATCH_SIZE": "100",
    "AWS_SERVING_URL_TTL_SECONDS": "300",
    "AWS_AUDIT_RETENTION_DAYS": "365",
    "AWS_PUBLIC_SUBNET_IDS": ",".join(PRODUCTION_SUBNET_IDS),
    "AWS_SECURITY_GROUP_ID": PRODUCTION_SECURITY_GROUP_ID,
    "OIDC_ISSUER": "https://cognito-idp.us-east-2.amazonaws.com/us-east-2_Example1",
    "OIDC_AUDIENCE": "https://api.cnesdata.com.br",
    "OIDC_DOMAIN": PRODUCTION_OIDC_DOMAIN,
})
