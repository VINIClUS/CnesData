"""Adapters do contrato de leads públicos."""

from cnes_infra.public_leads.dynamodb import DynamoDBPublicLeadStore
from cnes_infra.public_leads.sqlite import SQLitePublicLeadStore

__all__ = ["DynamoDBPublicLeadStore", "SQLitePublicLeadStore"]
