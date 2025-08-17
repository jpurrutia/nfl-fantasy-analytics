# Database Migrations

This directory contains SQL migration scripts for schema changes in the NFL Analytics database.

## Migration Naming Convention

Files are named as: `XXX_description.sql`
- `XXX` is a 3-digit version number (001, 002, etc.)
- `description` is a brief description using underscores

Example: `002_rename_tables_source_prefix.sql`

## Running Migrations

```bash
# Run all pending migrations
uv run python -m src.cli.main migrate

# Check migration status
uv run python -m src.cli.main migrate --status

# Rollback last migration (if supported)
uv run python -m src.cli.main migrate --rollback
```

## Migration History

| Version | Description | Date | Status |
|---------|------------|------|--------|
| 001 | Initial schema documentation | 2025-08-11 | Pending |
| 002 | Rename tables with source prefixes | 2025-08-11 | Pending |
| 003 | Add NGS and snap tables, restructure opportunity to silver | 2025-08-17 | Pending |

## Future: dbt Integration

In Phase 2.5, we'll transition to using dbt for data transformations:
- Bronze → Silver transformations will become dbt models
- Data quality tests will use dbt's testing framework
- Documentation will be auto-generated with dbt docs

For now, these simple SQL migrations handle schema changes effectively.