# DRAFT - NOT READY FOR USE
# This command is under development and should not be used yet
# Rename to analyze-player.md when implementation is complete

# Analyze Player Performance

Analyze player performance for: $ARGUMENTS

Follow the explore-plan-code-commit workflow:

## 1. EXPLORE - Understand the data
- Read bronze layer tables to understand available data:
  - `bronze.source_players` - Player information
  - `bronze.source_weekly_performance` - Weekly stats
  - `bronze.source_pbp_participation` - Play-by-play data
  - `bronze.source_ngs_receiving` / `source_ngs_rushing` / `source_ngs_passing` - Advanced metrics
- Check player name variations in `bronze.source_player_name_mapping`
- Identify relevant columns and metrics for analysis

## 2. PLAN - Think about the analysis approach
- Use "think" to consider analysis strategy
- Determine which metrics are most relevant:
  - Consistency metrics (CV, boom/bust rates)
  - Opportunity metrics (targets, carries, red zone usage)
  - Efficiency metrics (YPA, YPT, success rates)
- Consider league context (standard vs superflex)
- Plan visualizations or outputs needed

## 3. CODE - Implement the analysis
- Write SQL queries using `src/analytics/sql_runner.py`
- Use league-aware queries from `src/analytics/league_aware_queries.py`
- Create analysis in stages:
  1. Basic stats aggregation
  2. Advanced metrics calculation
  3. Trend analysis over time
  4. Comparison to position averages
- Save queries to `sql/queries/analytics/` for reuse

## 4. VALIDATE - Check results
- Run data quality checks on results
- Compare to known benchmarks
- Verify calculations are correct
- Test edge cases (injuries, bye weeks)

## 5. COMMIT - Save the analysis
- Document findings in PROJECT_LOG.md
- Save queries with descriptive names
- Update any relevant documentation
- Commit with clear message about analysis performed

Example usage:
- Single player: "Justin Jefferson 2024"
- Position group: "RB consistency analysis"
- Comparison: "Mahomes vs Allen QB efficiency"