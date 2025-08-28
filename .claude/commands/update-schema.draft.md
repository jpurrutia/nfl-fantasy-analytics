# DRAFT - NOT READY FOR USE
# This command is under development and should not be used yet
# Rename to update-schema.md when implementation is complete

# Update Database Schema

Update database schema and run migrations: $ARGUMENTS

Follow these steps for schema changes:

## 1. Review Current Schema
```bash
# Open DuckDB to inspect current schema
duckdb data/nfl_analytics.duckdb
.schema
```

## 2. Plan Schema Changes
- Document what tables need changes
- Identify new columns or tables needed
- Consider impacts on existing data
- Plan migration strategy

## 3. Create Migration Script
```sql
-- TODO: Create versioned migration files
-- migrations/YYYY-MM-DD_description.sql
-- Example structure:
-- BEGIN TRANSACTION;
-- ALTER TABLE bronze.source_players ADD COLUMN new_field VARCHAR;
-- UPDATE bronze.source_players SET new_field = ...;
-- COMMIT;
```

## 4. Backup Database
```bash
# Create backup before schema changes
cp data/nfl_analytics.duckdb data/nfl_analytics_backup_$(date +%Y%m%d).duckdb
```

## 5. Test Migration
- Run migration on test database first
- Verify data integrity after migration
- Check all queries still work

## 6. Apply Migration
```bash
# Run migration script
duckdb data/nfl_analytics.duckdb < migrations/[migration_file].sql
```

## 7. Update Code
- Update SQLAlchemy models if needed
- Modify queries for new schema
- Update data ingestion pipelines

## 8. Update Tests
- Add tests for new schema elements
- Update existing tests for schema changes
- Verify all tests pass

## 9. Document Changes
- Update schema documentation
- Record migration in PROJECT_LOG.md
- Update any affected README files

## Common Schema Updates:
- Adding indexes for performance
- New columns for additional stats
- Creating silver/gold layer tables
- Adding foreign key constraints
- Modifying data types

## Future Implementation Needed:
- [ ] Create migration framework with version tracking
- [ ] Add rollback capabilities
- [ ] Implement schema validation tests
- [ ] Create automated migration generator
- [ ] Add schema change impact analysis