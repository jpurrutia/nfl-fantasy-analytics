# https://www.anthropic.com/engineering/claude-code-best-practices

# NFL Analytics - Claude Code Context

## IMPORTANT: Core Commands
```bash
# Run tests FIRST (TDD approach)
uv run pytest tests/ -xvs

# Lint and format code
ruff check . --fix
ruff format .

# Run CLI
uv run python -m src.cli.main --help

# Database operations
uv run python -m src.cli.main status
uv run python -m src.cli.main validate --interactive
uv run python -m src.cli.main ingest --year 2024

# League-aware analytics (AUTO-DETECTS from ESPN)
uv run python -m src.cli.main league --league-id 537814 --show-config
uv run python -m src.cli.main transform run --layer silver player_consistency --league-id 537814
uv run python -m src.cli.main analyze consistency --league-id 537814 --position QB

# Draft tool (Interactive league-aware draft assistant)  
uv run python -m src.cli.main draft --league-id 537814
uv run python -m src.draft.main  # Direct access

# SQL query UI (MotherDuck)
duckdb data/nfl_analytics.duckdb -ui
```

## IMPORTANT: Development Workflow
1. **Write tests FIRST** - Follow TDD principles
2. **Run pre-commit** - `pre-commit run --all-files`
3. **Check types** - Ensure type hints are correct
4. **Validate data** - Run validation after any data changes

## CRITICAL: Python Execution
- **ALWAYS use `uv run`** - Never run `python` directly
- **All Python commands** - `uv run python -m src.cli.main`
- **Package management** - `uv pip install package`
- **Testing** - `uv run pytest tests/`
- **Scripts** - `uv run python script.py`

## Code Style (MUST FOLLOW)
- **Python >= 3.11** with type hints
- **PEP8 via Ruff** - 88 char lines, double quotes
- **Imports** - Use absolute imports from `src/`
- **Docstrings** - Google style for all public functions
- **Click decorators** - For all CLI commands

## Project Structure
- `src/cli/main.py` - CLI entry point
- `src/draft/` - League-aware draft tool
- `src/ingestion/` - Data pipelines
- `src/connectors/` - ESPN API
- `src/utils/` - Validation, DB init
- `data/nfl_analytics.duckdb` - Database

## Database Schema
- **bronze.** - Raw data (players, performance, mapping)
- **silver.** - Cleaned data (future)
- **gold.** - Analytics (future)

## Testing Requirements
- **ALWAYS write tests first** (TDD)
- Use pytest fixtures for database setup
- Mock external API calls
- Test data quality checks

## Interactive Development
- Ask user for decisions on analytics approaches
- Get feedback before implementing complex logic
- Validate results against user's fantasy knowledge
- Review each phase before proceeding

## Pre-commit Setup
```bash
# Install pre-commit
uv pip install pre-commit

# Install hooks
pre-commit install

# Run manually
pre-commit run --all-files
```

## League Configuration (AUTO-ADAPTIVE)
- **Auto-detection**: Connects to ESPN API and detects league format
- **Supported formats**: Standard, Superflex, OP, no-kickers, custom flex
- **Position validation**: Only shows positions available in your league
- **QB-flex aware**: Adjusts analytics for QB-eligible flex positions

## League Commands
```bash
# Auto-detect and cache league settings
uv run python -m src.cli.main league --league-id 537814 --show-config

# Force re-detection (clears cache)
uv run python -m src.cli.main league --league-id 537814 --reload

# Analytics adapt automatically to league format
uv run python -m src.cli.main analyze consistency --league-id 537814
```

## ESPN Integration
- **League ID**: 537814 (from config.yaml)
- **Auto-detection**: Roster positions, scoring type, flex positions
- **Credentials**: SWID and espn_s2 in config.yaml
- **Fallback**: Manual config if ESPN detection fails

## Current Status
- **Integrated Draft Tool**: Complete - fully integrated into main CLI
- **League-Aware Analytics**: Complete - adapts to any league format
- **Position Detection**: Auto-detected from ESPN API with fallback to manual config
- **Flex Handling**: FLEX vs OP vs SUPERFLEX properly distinguished
- **No Hardcoding**: All features skip positions not available in league

## Draft Tool Features
- **Real-time Recommendations**: ADP-based value calculations with league adjustments
- **Smart Strategy**: Automatically defers K/DST to late rounds
- **QB-Flex Optimization**: Enhanced QB values (1.5x) for superflex leagues
- **Draft Persistence**: Auto-save with backup system and undo functionality
- **Position Filtering**: Only shows players available in your league format
- **Interactive Commands**: `d [player]`, `o [player]`, `undo`, `status`, `q`

## Analytics Features
- **Dynamic SQL**: Generates queries based on league positions
- **QB-Flex Aware**: Enhanced analytics for superflex/OP leagues
- **Position Validation**: CLI only shows valid positions for league
- **Threshold Adaptation**: Boom/bust calculations adjust to league format

## NEVER DO
- **Run `python` directly** (always use `uv run python`)
- Hardcode position lists (use league.all_eligible_positions)
- Assume kickers exist (check league.has_kickers)
- Skip league validation in analytics commands
- Commit .env files or credentials
- Create files without tests
- Use relative imports