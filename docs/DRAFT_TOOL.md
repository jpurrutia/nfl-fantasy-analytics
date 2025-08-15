# Draft Tool Guide

> **League-Aware Interactive Draft Assistant**

The NFL Analytics draft tool automatically adapts to your league format and provides real-time, intelligent draft recommendations throughout your fantasy football draft.

## 🚀 Quick Start

### **Launch Draft Tool**
```bash
# Via main CLI (recommended)
uv run python -m src.cli.main draft --league-id YOUR_LEAGUE_ID

# Direct access
uv run python -m src.draft.main
```

### **ESPN Credentials Setup**
Add your credentials to `config/config.yaml`:
```yaml
espn:
  league_id: 537814
  swid: "YOUR_SWID_COOKIE"
  espn_s2: "YOUR_ESPN_S2_COOKIE"
```

**Finding Credentials:**
1. Log into ESPN Fantasy
2. Open browser dev tools (F12) 
3. Go to Application → Cookies
4. Copy `SWID` and `espn_s2` values

## 🎯 Draft Commands

| **Command** | **Action** | **Example** |
|-------------|------------|-------------|
| `d [player]` | Draft player to your team | `d Josh Allen` |
| `o [player]` | Mark player drafted by others | `o Ja'Marr Chase` |
| `undo` | Undo your last pick | `undo` |
| `status` | Show draft progress | `status` |
| `save` | Manual save draft state | `save` |
| `load` | Load from backup | `load` |
| `q` | Quit and save | `q` |

## 🏈 League-Aware Features

### **Automatic League Detection**
The tool connects to ESPN and detects:
- **Roster positions** (QB, RB, WR, TE, FLEX, OP, K, DST)
- **Flex types** (FLEX vs OP vs SUPERFLEX)
- **Scoring format** (PPR, Half-PPR, Standard)
- **League size** (8, 10, 12+ teams)

### **Position Filtering** 
- Only shows players from positions in your league
- Filters out kickers if your league doesn't use them
- Validates draft picks against league eligibility

### **QB-Flex Intelligence**
For superflex/OP leagues:
- **Enhanced QB values** (1.5x multiplier)
- **QB scarcity calculations** 
- **Special QB-flex indicators** in recommendations
- **Adjusted boom/bust thresholds**

## 📊 Recommendations System

### **Value Calculation**
Each recommendation shows:
```
Josh Allen (QB) - BUF ⭐ [FILLS STARTER NEED] [QB-FLEX VALUE]
  Tier: 1 | ADP: 5.7 | Proj: 498
  Value: 5.3 | Need: 20 | TOTAL: 14.1
  Verdict: GOOD VALUE
```

- **ADP**: League-adjusted Average Draft Position
- **Value**: Current pick - ADP (positive = good value)
- **Need**: Position importance score (20 = critical starter need)
- **Total**: Combined value + need score

### **Smart Strategy**
- **Early Rounds**: Focus on skill positions (QB, RB, WR, TE)
- **Late Rounds**: Automatically surfaces K/DST when appropriate
- **Position Balance**: Prevents over-drafting at saturated positions
- **Tier Awareness**: Highlights tier breaks and value opportunities

## 🎯 Draft Interface

### **Draft Board Display**
```
============================================================
PICK 93 - ROUND 10/18 - LEAGUE-AWARE RECOMMENDATIONS  
============================================================
🏈 Weenieless Wanderers (PPR)
📊 Positions: QB, RB, WR, TE, DST (no kickers)
⚡ QB-Flex league: Enhanced QB values
```

### **Roster Status**
Shows your current roster with:
- **Starting positions** filled/empty
- **Bench spots** available
- **Critical needs** highlighted
- **Deferred positions** (K/DST) noted

### **Position Summary**
```
📊 Position Summary (League-Aware):
  Position | Starters | Bench | Total | Status
  ---------|----------|-------|-------|--------
  QB       |    2     |   1   |   3   | ✓
  RB       |    4     |   2   |   6   | ✓  
  WR       |    2     |   1   |   3   | Need 2
  TE       |    1     |   0   |   1   | Need 1
  K        |    0     |   0   |   0   | Wait
  D/ST     |    0     |   0   |   0   | Wait
```

## 🔧 Advanced Features

### **Draft State Persistence**
- **Auto-save** after each pick
- **Timestamped backups** (keeps last 10)
- **Resume capability** from any point
- **Undo functionality** for last pick

### **Player Matching**
- **Fuzzy search** - partial names work
- **Defense handling** - automatically detects team defenses
- **Validation** - prevents invalid picks
- **Suggestions** - shows similar names if no match

### **League Format Examples**

**Standard League:**
```
🎯 Roster: 1QB, 2RB, 2WR, 1TE, 1FLEX, 1K, 1DST, 6BN
📊 QB Multiplier: 1.0x (standard value)
```

**Superflex League:**
```  
🎯 Roster: 1QB, 2RB, 2WR, 1TE, 2FLEX, 1OP, 1DST, 7BN
📊 QB Multiplier: 1.5x (enhanced value)
⚡ QB-Flex league: 2 QB slots total
```

**No-Kicker League:**
```
🎯 Roster: 1QB, 2RB, 3WR, 1TE, 1FLEX, 1DST, 6BN  
🚫 No kickers - Position filtered out entirely
```

## 📈 Value Philosophy

### **ADP-Based Recommendations**
- Uses FantasyPros consensus ADP as baseline
- Adjusts for your specific league format
- Factors in positional scarcity
- Weights current team needs

### **Tier-Based Drafting**
- Identifies natural tier breaks
- Recommends best available within tiers
- Warns when reaching for players
- Highlights exceptional value picks

### **Positional Strategy**
- **Skill positions first** (QB, RB, WR, TE)
- **Kickers/Defense late** (rounds 15-18)
- **Handcuff awareness** for RB backups
- **Bye week considerations** for key positions

## 🚨 Troubleshooting

### **Common Issues**

**"Legacy Mode" Message:**
- ESPN credentials missing or invalid
- Falls back to manual league configuration
- Update `config/config.yaml` with valid cookies

**"Player not found":**
- Use partial names (e.g., "Josh Allen" or just "Allen")
- Check spelling and try fuzzy matching
- Tool will suggest similar names

**"Position not available":**
- Player's position doesn't exist in your league
- Common with kickers in no-K leagues
- Tool prevents invalid drafts

### **Getting Help**
```bash
# Check league configuration
uv run python -m src.cli.main league --league-id YOUR_LEAGUE_ID --show-config

# Validate ESPN connection
uv run python -m src.cli.main league --league-id YOUR_LEAGUE_ID --reload

# Check draft tool directly
uv run python -m src.draft.main
```

## 🏆 Pro Tips

### **Draft Strategy**
1. **Trust the value scores** - positive values are good picks
2. **Fill critical needs first** - starred recommendations
3. **Don't reach for kickers** - tool will remind you when it's time
4. **Use undo freely** - experiment with different picks
5. **Save manually** before risky picks

### **League Format Optimization**
- **Superflex**: QBs become much more valuable early
- **No-Kicker**: Extra roster spot for skill positions
- **Large Leagues**: Deeper benches, more speculative picks
- **PPR**: WRs get value boost, especially in PPR formats

---

**Ready to dominate your draft with league-aware intelligence! 🚀**

```bash
uv run python -m src.cli.main draft --league-id YOUR_LEAGUE_ID
```