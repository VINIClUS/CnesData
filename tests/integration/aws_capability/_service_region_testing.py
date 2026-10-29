"""Metadata regional literal para exercitar o janitor sem alterar contratos."""

from collections.abc import Mapping
from dataclasses import dataclass

from tests.integration.aws_capability.contracts import Ownership

REGION_TAGS = {
    "Project": "CnesData",
    "Purpose": "aws-capability",
    "Run": "a1b2c3d4",
    "ExpiresAt": "1060",
}


@dataclass(frozen=True)
class RegionalOwnership(Ownership):
    region: str


@dataclass(frozen=True)
class RegionlessOwnership:
    account: str
    tags: Mapping[str, str]
