# NFL Analytics Platform

> **League-Aware Fantasy Football Analytics & Draft Tool**

Data-driven fantasy football platform that automatically adapts to your league format, providing personalized analytics, player projections, and an intelligent draft assistant.

## 🏈 Core Features

### **📊 League-Aware Analytics**
- **Auto-detects** your ESPN league format (superflex, no-kickers, custom positions)
- **Dynamic analytics** that adapt to your specific league rules
- **Position-specific insights** based on scarcity and value in your format

### **🎯 Interactive Draft Tool**  
- **Real-time recommendations** with ADP-based value calculations
- **League format optimization** (enhanced QB values for superflex leagues)
- **Smart position strategy** (defers K/DST to late rounds automatically)
- **Draft state persistence** with auto-save and backup functionality

### **🔧 Data Pipeline**
- **NFL data ingestion** via nfl-data-py
- **ESPN league integration** with automatic configuration detection  
- **DuckDB analytics engine** with SQL-first approach
- **Comprehensive data validation** and quality checks

## 🚀 Quick Start

### **Installation**
```bash
# Clone and setup
git clone <repository-url>
cd nfl-analytics

# Install with uv (recommended)
uv sync
```

### **Configuration**
```bash
# Add your ESPN credentials to config/config.yaml
espn:
  league_id: YOUR_LEAGUE_ID
  swid: "YOUR_SWID_COOKIE"
  espn_s2: "YOUR_ESPN_S2_COOKIE"
```

### **First Time Setup**
```bash
# Initialize database and ingest data
uv run python -m src.cli.main ingest --year 2024

# Verify setup
uv run python -m src.cli.main status
```

## 🎯 Usage

### **🏈 Draft Tool (Main Feature)**
```bash
# Start interactive draft tool (auto-detects your league)
uv run python -m src.cli.main draft --league-id YOUR_LEAGUE_ID

# Or run directly
uv run python -m src.draft.main
```

**Draft Commands:**
- `d Josh Allen` - Draft player to your team
- `o Ja'Marr Chase` - Mark player drafted by others  
- `undo` - Undo your last pick
- `status` - Show draft progress
- `q` - Quit and save

### **📊 Analytics Commands**
```bash
# League configuration (auto-detection)
uv run python -m src.cli.main league --league-id YOUR_LEAGUE_ID --show-config

# Player consistency analysis (league-aware)
uv run python -m src.cli.main analyze consistency --league-id YOUR_LEAGUE_ID --position QB

# Boom/bust analysis
uv run python -m src.cli.main analyze boom-bust --league-id YOUR_LEAGUE_ID --position RB
```

### **🔧 Data Management**
```bash
# Data ingestion
uv run python -m src.cli.main ingest --year 2024 --historical

# Data validation  
uv run python -m src.cli.main validate --interactive

# Database status
uv run python -m src.cli.main status
```

### **🗄️ SQL Interface**
```bash
# Interactive SQL with MotherDuck UI
duckdb data/nfl_analytics.duckdb -ui

# Pre-built queries available in sql/queries/
```

## 📁 Project Structure

```
src/
├── cli/main.py          # Main CLI interface
├── draft/               # Interactive draft tool
│   ├── main.py         # Draft logic & recommendations
│   ├── data/           # ADP files & draft data
│   └── backups/        # Draft state persistence
├── analytics/           # SQL analytics engine
├── connectors/          # ESPN API integration  
├── ingestion/           # Data pipelines
└── utils/               # League config & validation

data/
├── nfl_analytics.duckdb # Main database
└── bronze/silver/gold/  # Data layers

sql/
├── queries/             # Pre-built analytics queries
├── transformations/     # Data transformation logic
└── practice/            # SQL learning challenges
```

## 🎯 League Format Support

The platform automatically adapts to **any league format**:

| **Format** | **Detection** | **Features** |
|------------|---------------|--------------|
| **Standard** | ✅ Auto | Standard player values & recommendations |
| **Superflex/OP** | ✅ Auto | Enhanced QB values (1.5x multiplier) |
| **No Kickers** | ✅ Auto | Filters out K positions entirely |
| **Custom Flex** | ✅ Auto | Supports FLEX, WR/TE, RB/WR variations |
| **Dynasty/Keeper** | ✅ Manual | Custom configuration support |

## 🔧 Advanced Usage

### **League Configuration Override**
```yaml
# config/config.yaml - Manual league configuration
league:
  scoring_type: "PPR"
  roster_positions:
    QB: 1
    RB: 2  
    WR: 2
    TE: 1
    FLEX: 2
    OP: 1      # QB-eligible superflex
    DST: 1     # No kickers
    BENCH: 7
```

### **Custom Analytics**
```bash
# Run transformations with league context
uv run python -m src.cli.main transform run --layer silver player_consistency --league-id YOUR_LEAGUE_ID

# Position-specific analysis  
uv run python -m src.cli.main analyze consistency --position QB --min-games 10 --league-id YOUR_LEAGUE_ID
```

## 🏗️ Development Status

### ✅ **Phase 1: Complete**
- Core data pipeline (NFL → DuckDB)
- ESPN league integration & auto-detection
- League-aware analytics engine
- Interactive draft tool

### ✅ **Phase 2: Complete** 
- Modular league configuration system
- Position-aware analytics & filtering
- Integrated CLI interface
- Comprehensive draft tool

### 🔄 **Phase 3: In Progress**
- Advanced projections & modeling
- Historical analysis & trends
- Web interface (planned)

## 📚 Documentation

- **[CLAUDE.md](CLAUDE.md)** - Development context & commands
- **[docs/DRAFT_TOOL.md](docs/DRAFT_TOOL.md)** - Comprehensive draft tool guide
- **[DuckDB_LIMITATIONS.md](DuckDB_LIMITATIONS.md)** - Database considerations

## 💡 Philosophy

**League-First Design**: Every feature adapts to your specific league format rather than forcing you to adapt to the tool.

**SQL-First Analytics**: Python orchestrates, SQL analyzes. Leverages DuckDB's performance for heavy lifting.

**Data Quality Focus**: Comprehensive validation ensures reliable insights and recommendations.

---

**Ready to dominate your fantasy draft? 🏆**

```bash
uv run python -m src.cli.main draft --league-id YOUR_LEAGUE_ID
```