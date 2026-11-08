"""Vocabulário dos modos configuráveis do runtime."""
from enum import StrEnum


class RuntimeProfile(StrEnum):
    LOCAL = "local"
    AWS = "aws"

class AuthMode(StrEnum):
    LOCAL = "local"
    OIDC = "oidc"

class BillingMode(StrEnum):
    DISABLED = "disabled"
    STRIPE = "stripe"
