# NFL Analytics

Fantasy football analytics platform providing data-driven insights for lineup optimization and player analysis.

## Features

- ESPN fantasy league integration
- Advanced player analytics and projections
- Lineup recommendations
- Interactive CLI interface
- DuckDB-powered analytics engine

## Installation

```bash
# Create virtual environment with Python 3.11
uv venv --python 3.11
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -r requirements.txt
```

## Configuration

1. Copy `.env.example` to `.env`
2. Add your ESPN credentials
3. Run database initialization:
   ```bash
   uv run python src/utils/db_init.py
   ```

## Usage

```bash
# CLI interface
uv run python -m src.cli.main --help

# Initialize database
uv run python src/utils/db_init.py
```

## Development

This project is built incrementally following KISS, DRY, SOLID, and YAGNI principles.

Current stage: MVP - Building core data foundation and analytics engine.