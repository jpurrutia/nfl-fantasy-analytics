# SQL Practice with NFL Play-by-Play Data

Learn SQL by analyzing real NFL play-by-play data! These queries are designed to teach SQL concepts while providing actual football insights.

## 📚 Learning Path

### Level 1: Fundamentals
1. **`pbp/01_filtering_basics.sql`** - WHERE clauses, basic filtering
2. **`pbp/02_aggregations.sql`** - GROUP BY, COUNT, SUM, AVG
3. **`pbp/03_joins_practice.sql`** - JOIN operations with player data

### Level 2: Intermediate
4. **`pbp/04_window_functions.sql`** - RANK, ROW_NUMBER, running totals

### Level 3: Real Analysis
5. **`pbp/05_qb_performance_analysis.sql`** - QB analysis by situation
6. **`pbp/06_red_zone_analysis.sql`** - Red zone efficiency deep dive
7. **`pbp/07_receiver_target_analysis.sql`** - WR/TE target distribution

## 🚀 How to Use

### Option 1: DuckDB UI (Recommended for exploration)
```bash
# Open interactive UI
duckdb data/nfl_analytics.duckdb -ui

# Copy and paste queries from the SQL files
```

### Option 2: Command Line
```bash
# Run entire file
duckdb data/nfl_analytics.duckdb < sql/practice/pbp/01_filtering_basics.sql

# Run specific query (copy/paste into terminal)
duckdb data/nfl_analytics.duckdb
```

### Option 3: VS Code / Editor
1. Open the SQL file in your editor
2. Copy the query you want to run
3. Paste into DuckDB UI or CLI

## 📊 Available Data

The `bronze.nfl_play_by_play` table contains:
- **49,665 plays** from the 2023 NFL season
- **80+ columns** including play details, player IDs, advanced metrics (EPA, CPOE)
- Perfect for learning filtering, aggregation, and complex analysis

## 💡 Learning Tips

1. **Start Simple**: Begin with `01_filtering_basics.sql`
2. **Modify Queries**: Each query has "Try modifying" suggestions
3. **Check Results**: Verify your results make football sense
4. **Practice Challenges**: Each file ends with practice exercises

## 🏈 Key Columns to Know

- `posteam` / `defteam` - Offensive and defensive teams
- `down`, `ydstogo` - Down and distance
- `play_type` - pass, run, punt, etc.
- `yards_gained` - Result of the play
- `epa` - Expected Points Added (advanced metric)
- `passer_player_name`, `receiver_player_name`, `rusher_player_name` - Players involved

## 📈 Load More Data

To load additional seasons for analysis:
```bash
# Load 2021-2024 seasons (takes ~5 minutes)
uv run python -m src.cli.main ingest-pbp --years 2021 2022 2023 2024
```

## 🎯 Next Steps

After completing these exercises:
1. Write your own analysis queries
2. Combine play-by-play with weekly performance data
3. Create views for commonly used metrics
4. Export results for visualization

Happy querying! 🚀