# DRAFT - NOT READY FOR USE
# This command is under development and should not be used yet
# Rename to data-quality-check.md when implementation is complete

# Data Quality Check

Run comprehensive data quality checks: $ARGUMENTS

Follow these steps to validate data integrity:

## 1. Check Database Connection
```bash
uv run python -m src.cli.main status
```
- Verify database is accessible
- Check table counts
- Review last update timestamps

## 2. Run Interactive Validation
```bash
uv run python -m src.cli.main validate --interactive
```
- Review data completeness by week
- Check for missing players
- Identify data gaps

## 3. Validate Player Mappings [TODO: Add specific queries]
- Check `bronze.source_player_name_mapping` for duplicates
- Verify all active players have gsis_id
- Ensure name variations are captured

## 4. Check Weekly Data Integrity [TODO: Implement SQL checks]
```sql
-- TODO: Add specific DuckDB queries for:
-- - Weeks with abnormal game counts
-- - Players with missing stats
-- - Duplicate entry detection
```

## 5. Verify Position Assignments
- Ensure all players have valid positions
- Check for position changes mid-season
- Validate against league roster positions

## 6. Test Data Relationships
- Verify player_id consistency across tables
- Check foreign key relationships
- Validate week/season combinations

## 7. Generate Quality Report
- Document issues found
- Create action items for fixes
- Update PROJECT_LOG.md with findings

## Future Implementation Needed:
- [ ] Add specific SQL validation queries
- [ ] Integrate with existing validation utilities
- [ ] Create automated quality report generation
- [ ] Add threshold-based alerts for data issues
- [ ] Implement data repair suggestions
- [ ] Connect to actual data quality framework