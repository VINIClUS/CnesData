# Code Reviewer Memory Index

## Recurring Violations

- [recurring_violations.md](recurring_violations.md) — Patterns that appear repeatedly across reviews; pay extra attention to these.
- [pattern_drift_contract_changes.md](pattern_drift_contract_changes.md) — Contract-only commits (cnes_contracts/landing.py, protocols.py) routinely leave central_api + cnes_infra tests red; always grep downstream consumers.
- [pattern_drift_migration_downgrade.md](pattern_drift_migration_downgrade.md) — Drop+recreate migrations must reconstruct the EXACT prior CREATE TABLE in downgrade (col names, constraint names, allowed values, storage params), not a plausible approximation.
