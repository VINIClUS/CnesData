"""Adapters do contrato de leads públicos."""

from cnes_data.public_leads.dynamodb import DynamoDBPublicLeadStore
from cnes_data.public_leads.sqlite import SQLitePublicLeadStore

__all__ = ["DynamoDBPublicLeadStore", "SQLitePublicLeadStore"]
