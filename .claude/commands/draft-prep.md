# Prepare for Draft

Prepare draft analysis for league: $ARGUMENTS

Follow these steps to ensure draft readiness:

## 1. Sync League Settings
- Run `uv run python -m src.cli.main league --league-id $ARGUMENTS --show-config`
- Verify roster positions are correctly detected
- Check if superflex/OP league detected properly
- Confirm scoring settings (PPR, Half-PPR, Standard)

## 2. Update Player Data
- Ingest latest weekly performance data:
  ```bash
  uv run python -m src.cli.main ingest --year 2024
  ```
- Validate data completeness:
  ```bash
  uv run python -m src.cli.main validate --interactive
  ```

## 3. Update ADP Rankings [NOT OPERATIONAL - MANUAL PROCESS]

**⚠️ NOTE: Auto-download not yet implemented. Follow manual process below.**

### Determine which ADP file to download based on league format:
- **PPR League**: Download PPR rankings
- **Half-PPR League**: Download Half-PPR rankings  
- **Standard League**: Download Standard rankings
- **Superflex/2QB League**: Download Superflex rankings

### Manual download process (TEMPORARY):
```bash
# TODO: Implement auto-download in future version
# Currently requires manual download from FantasyPros
# Save to src/draft/data/ with appropriate naming
```

### Verify ADP data:
- Check current files in `src/draft/data/`
- Verify player name mappings are current
- Confirm ADP values are recent

## 4. Run Position Analysis
- Generate consistency reports for each position:
  ```bash
  uv run python -m src.cli.main analyze consistency --league-id $ARGUMENTS --position QB
  uv run python -m src.cli.main analyze consistency --league-id $ARGUMENTS --position RB
  uv run python -m src.cli.main analyze consistency --league-id $ARGUMENTS --position WR
  uv run python -m src.cli.main analyze consistency --league-id $ARGUMENTS --position TE
  ```
- Review boom/bust rates for target players
- Identify value picks by position

## 5. Test Draft Tool
- Launch draft tool in test mode:
  ```bash
  uv run python -m src.cli.main draft --league-id $ARGUMENTS
  ```
- Verify recommendations align with league format
- Test undo functionality
- Check auto-save is working

## 6. Generate Draft Strategy
- Document top targets by round
- Note position runs to watch for
- Identify late-round values
- Plan QB strategy based on league type (standard vs superflex)

## 7. Update PROJECT_LOG.md
- Document draft prep completion
- Note any data issues found
- List key insights discovered
- Record draft date/time if known

## Future Enhancement
- Implement auto-download of league-specific ADP data
- Pull PPR/Standard/Half-PPR rankings based on league.scoring_type
- Auto-refresh before each draft session