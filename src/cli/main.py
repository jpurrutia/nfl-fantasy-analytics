#!/usr/bin/env python3
"""
NFL Analytics CLI - Main entry point

This provides a simple command-line interface for the NFL Analytics platform.
Currently supports data ingestion and quality validation.

Usage:
    python -m src.cli.main --help
    python -m src.cli.main ingest --year 2024
    python -m src.cli.main validate
    python -m src.cli.main map-players
"""

import click
import sys
from pathlib import Path
import logging

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.ingestion.nfl_data import NFLDataIngestion
from src.ingestion.player_mapping import PlayerMapper
from src.utils.data_quality import DataQualityValidator
from src.utils.migration import MigrationRunner
from src.analytics.sql_runner import SQLRunner, SQLRunnerError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@click.group()
@click.version_option(version="0.1.0")
def cli():
    """NFL Analytics Platform CLI
    
    A data-driven fantasy football analytics platform.
    """
    pass

@cli.command()
@click.option('--year', '-y', type=int, default=2024, help='Year to ingest data for')
@click.option('--historical', is_flag=True, help='Include previous year for historical analysis')
def ingest(year: int, historical: bool):
    """Ingest NFL data from nfl-data-py into bronze tables."""
    
    years = [year]
    if historical:
        years.append(year - 1)
    
    click.echo(f"🏈 Starting NFL data ingestion for {years}")
    
    try:
        ingestion = NFLDataIngestion()
        
        # Load players
        player_count = ingestion.load_players(years)
        click.echo(f"✅ Loaded {player_count} players")
        
        # Load performance data
        perf_count = ingestion.load_player_performance(years)
        click.echo(f"✅ Loaded {perf_count} performance records")
        
        # Validate
        validation_results = ingestion.validate_data()
        click.echo(f"✅ Validation complete - {validation_results['latest_data']}")
        
        click.echo("🎉 Data ingestion successful!")
        
    except Exception as e:
        click.echo(f"❌ Ingestion failed: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--interactive', is_flag=True, help='Generate SQL for interactive exploration')
def validate(interactive: bool):
    """Run data quality validation."""
    
    try:
        validator = DataQualityValidator()
        
        if interactive:
            click.echo("📋 Generated SQL for interactive exploration:")
            click.echo("-" * 50)
            sql_query = validator.generate_quick_check_sql()
            click.echo(sql_query)
            click.echo("\n💡 Copy this SQL and run in your DuckDB CLI!")
        else:
            click.echo("🔍 Running automated validation...")
            results = validator.run_automated_validation()
            
            click.echo(f"\n📊 VALIDATION RESULTS - {results['overall_status']}")
            for check in results['checks']:
                status_icon = "✅" if check['status'] == 'PASS' else "❌"
                click.echo(f"{status_icon} {check['check']}: {check['message']}")
            click.echo(f"\nSummary: {results['summary']}")
        
    except Exception as e:
        click.echo(f"❌ Validation failed: {e}", err=True)
        sys.exit(1)

@cli.command('map-players')
def map_players():
    """Test the player mapping system with sample data."""
    
    import pandas as pd
    
    try:
        mapper = PlayerMapper()
        
        # Sample ESPN players
        sample_players = pd.DataFrame([
            {'id': '1', 'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF'},
            {'id': '2', 'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'},
            {'id': '3', 'name': 'Justin Jefferson', 'position': 'WR', 'team': 'MIN'},
            {'id': '4', 'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC'},
            {'id': '5', 'name': 'Patrick Mahomes', 'position': 'QB', 'team': 'KC'}
        ])
        
        click.echo("🔗 Testing player mapping with sample data...")
        
        # Perform mapping
        mappings = mapper.map_espn_to_nfl(sample_players)
        
        if not mappings.empty:
            # Save mappings
            mapper.save_mappings(mappings)
            
            # Show results
            espn_mappings = mappings[mappings['platform'] == 'ESPN']
            click.echo(f"✅ Successfully mapped {len(espn_mappings)} players:")
            for _, mapping in espn_mappings.iterrows():
                click.echo(f"  • {mapping['player_name']} → {mapping['player_name_variant']}")
            
            # Get stats
            stats = mapper.get_mapping_stats()
            click.echo(f"\n📊 Total mappings in database: {stats['total_mappings']}")
        else:
            click.echo("❌ No mappings created")
            
    except Exception as e:
        click.echo(f"❌ Mapping failed: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--status', is_flag=True, help='Show migration status')
@click.option('--rollback', is_flag=True, help='Rollback last migration')
def migrate(status: bool, rollback: bool):
    """Manage database migrations."""
    
    try:
        runner = MigrationRunner()
        
        if status:
            # Show migration status
            migration_status = runner.get_status()
            click.echo("=" * 50)
            click.echo("📋 Migration Status")
            click.echo("=" * 50)
            
            for migration in migration_status:
                status_icon = "✅" if migration['status'] == 'applied' else "⏳"
                click.echo(f"{status_icon} {migration['version']:03d}_{migration['name']}: {migration['status']}")
                if migration['applied_at']:
                    click.echo(f"    Applied: {migration['applied_at']}")
            
        elif rollback:
            # Rollback last migration
            click.echo("🔄 Rolling back last migration...")
            if runner.rollback_last():
                click.echo("✅ Rollback successful")
            else:
                click.echo("❌ Rollback failed (see logs for details)")
                sys.exit(1)
        
        else:
            # Apply pending migrations
            click.echo("🚀 Applying pending migrations...")
            count = runner.run_migrations()
            if count > 0:
                click.echo(f"✅ Applied {count} migration(s) successfully")
            else:
                click.echo("✅ No pending migrations")
    
    except Exception as e:
        click.echo(f"❌ Migration error: {e}", err=True)
        sys.exit(1)

@cli.command()
def status():
    """Show current database status and data summary."""
    
    try:
        validator = DataQualityValidator()
        results = validator.run_automated_validation()
        
        click.echo("=" * 50)
        click.echo("🏈 NFL ANALYTICS DATABASE STATUS")
        click.echo("=" * 50)
        
        # Show validation results
        for check in results['checks']:
            status_icon = "✅" if check['status'] == 'PASS' else "❌"
            click.echo(f"{status_icon} {check['check']}: {check['message']}")
        
        click.echo(f"\nOverall Status: {results['overall_status']}")
        click.echo(f"Last Updated: {results['timestamp']}")
        
        click.echo("\n💡 Next steps:")
        click.echo("  • Try: python -m src.cli.main validate --interactive")
        click.echo("  • Explore: sql/practice/data_quality_challenges.sql")
        click.echo("  • Analyze: sql/queries/data_quality/")
        
    except Exception as e:
        click.echo(f"❌ Status check failed: {e}", err=True)
        sys.exit(1)

@cli.group()
def transform():
    """Run SQL transformations to create silver/gold layers."""
    pass

@transform.command('list')
@click.option('--layer', type=click.Choice(['silver', 'gold']), default='silver', help='Data layer to list')
def list_transformations(layer: str):
    """List available transformations."""
    try:
        runner = SQLRunner()
        transformations = runner.get_available_transformations(layer)
        
        if transformations:
            click.echo(f"📋 Available {layer} transformations:")
            for t in transformations:
                click.echo(f"  • {t}")
        else:
            click.echo(f"No transformations found for {layer} layer")
            
    except Exception as e:
        click.echo(f"❌ Error listing transformations: {e}", err=True)
        sys.exit(1)

@transform.command('run')
@click.option('--layer', type=click.Choice(['silver', 'gold']), required=True, help='Data layer')
@click.option('--league-id', help='League ID for configuration')
@click.option('--season', type=int, help='Season filter')
@click.option('--min-games', type=int, help='Minimum games filter')
@click.argument('transformation_name')
def run_transformation(layer: str, transformation_name: str, league_id: str, season: int, min_games: int):
    """Run a league-aware transformation."""
    try:
        runner = SQLRunner(league_id=league_id)
        
        # Show league info
        league_info = runner.get_league_info()
        click.echo(f"🏈 League: {league_info['league_name']} ({league_info['scoring_type']})")
        click.echo(f"📊 Positions: {', '.join(league_info['positions'])}")
        
        click.echo(f"🔄 Running {layer}/{transformation_name} transformation...")
        runner.run_transformation(
            layer, 
            transformation_name, 
            season=season, 
            min_games=min_games
        )
        click.echo("✅ Transformation complete!")
        
    except SQLRunnerError as e:
        click.echo(f"❌ Transformation failed: {e}", err=True)
        sys.exit(1)

@cli.group()
def analyze():
    """Run analytics queries on transformed data."""
    pass

@analyze.command('consistency')
@click.option('--position', help='Filter by position (auto-detected from league)')
@click.option('--league-id', help='League ID for configuration')
@click.option('--min-games', type=int, default=8, help='Minimum games played')
@click.option('--limit', type=int, default=20, help='Number of results to show')
@click.option('--format', type=click.Choice(['table', 'json', 'csv']), default='table', help='Output format')
@click.option('--season', type=int, help='Season filter')
def analyze_consistency(position: str, league_id: str, min_games: int, limit: int, format: str, season: int):
    """Analyze player consistency metrics (league-aware)."""
    try:
        runner = SQLRunner(league_id=league_id)
        
        # Show available positions for this league
        available_positions = runner.get_available_positions()
        league_info = runner.get_league_info()
        
        click.echo(f"🏈 League: {league_info['league_name']} ({league_info['scoring_type']})")
        click.echo(f"📊 Available positions: {', '.join(available_positions)}")
        
        # Validate position if provided
        if position and position not in available_positions:
            click.echo(f"❌ Position '{position}' not available in this league.")
            click.echo(f"💡 Available: {', '.join(available_positions)}")
            sys.exit(1)
        
        # Run league-aware query
        df = runner.run_league_aware_query(
            "consistency",
            position=position,
            season=season,
            min_games=min_games
        )
        
        # Apply limit
        if limit:
            df = df.head(limit)
        
        title = f"📊 Top {len(df)} Most Consistent Players"
        if position:
            title += f" ({position})"
        if league_info['has_qb_flex'] and position == 'QB':
            title += " (QB-Flex League)"
        
        click.echo(title)
        click.echo("=" * 50)
        
        output = runner.format_results(df, format=format)
        click.echo(output)
        
        # Show league context if QB-flex
        if league_info['has_qb_flex']:
            click.echo(f"\n💡 QB slots: {league_info['qb_slots']} (includes flex)")
        
    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}", err=True)
        sys.exit(1)

@analyze.command('boom-bust')
@click.option('--position', help='Filter by position (auto-detected from league)')
@click.option('--league-id', help='League ID for configuration')
@click.option('--sort-by', type=click.Choice(['boom', 'bust', 'consistency']), default='boom', help='Sort criteria')
@click.option('--limit', type=int, default=20, help='Number of results to show')
@click.option('--season', type=int, help='Season filter')
def analyze_boom_bust(position: str, league_id: str, sort_by: str, limit: int, season: int):
    """Analyze boom/bust potential of players (league-aware)."""
    try:
        runner = SQLRunner(league_id=league_id)
        
        # Show available positions for this league
        available_positions = runner.get_available_positions()
        league_info = runner.get_league_info()
        
        click.echo(f"🏈 League: {league_info['league_name']} ({league_info['scoring_type']})")
        click.echo(f"📊 Available positions: {', '.join(available_positions)}")
        
        # Validate position if provided
        if position and position not in available_positions:
            click.echo(f"❌ Position '{position}' not available in this league.")
            click.echo(f"💡 Available: {', '.join(available_positions)}")
            sys.exit(1)
        
        # Run league-aware query
        df = runner.run_league_aware_query(
            "consistency",
            position=position,
            season=season,
            min_games=8
        )
        
        # Sort by criteria
        sort_map = {
            'boom': 'boom_rate_pct',
            'bust': 'bust_rate_pct', 
            'consistency': 'consistency_score'
        }
        ascending = sort_by == 'bust'  # Bust rate: lower is better
        df = df.sort_values(sort_map[sort_by], ascending=ascending)
        
        # Apply limit
        if limit:
            df = df.head(limit)
        
        # Select relevant columns
        columns = [
            'name', 'team', 'position', 'n_games', 'avg_performance_pts',
            'boom_rate_pct', 'bust_rate_pct', 'ceiling', 'floor'
        ]
        df_display = df[columns].round(1)
        
        title_map = {
            'boom': 'Highest Boom Potential',
            'bust': 'Lowest Bust Rate',
            'consistency': 'Most Consistent'
        }
        
        title = f"💥 {title_map[sort_by]} Players"
        if position:
            title += f" ({position})"
        if league_info['has_qb_flex'] and position == 'QB':
            title += " (QB-Flex League)"
        
        click.echo(title)
        click.echo("=" * 50)
        
        output = runner.format_results(df_display, format="table")
        click.echo(output)
        
        # Show league context
        if league_info['has_qb_flex']:
            click.echo(f"\n💡 QB slots: {league_info['qb_slots']} (includes flex)")
        
    except Exception as e:
        click.echo(f"❌ Analysis failed: {e}", err=True)
        sys.exit(1)

@cli.command('league')
@click.option('--league-id', help='League ID to configure')
@click.option('--show-config', is_flag=True, help='Show current league configuration')
@click.option('--reload', is_flag=True, help='Reload league configuration')
def league_config(league_id: str, show_config: bool, reload: bool):
    """Manage league configuration."""
    try:
        runner = SQLRunner(league_id=league_id)
        
        if reload:
            click.echo("🔄 Reloading league configuration...")
            runner.reload_league_config(league_id, force_detection=True)
        
        if show_config or reload:
            league_info = runner.get_league_info()
            
            click.echo("=" * 50)
            click.echo("🏈 LEAGUE CONFIGURATION")
            click.echo("=" * 50)
            click.echo(f"League ID: {league_info['league_id']}")
            click.echo(f"League Name: {league_info['league_name']}")
            click.echo(f"Scoring: {league_info['scoring_type']}")
            click.echo(f"Size: {league_info['league_size']} teams")
            click.echo(f"Positions: {', '.join(league_info['positions'])}")
            click.echo(f"QB-Flex Eligible: {'Yes' if league_info['has_qb_flex'] else 'No'}")
            click.echo(f"Has Kickers: {'Yes' if league_info['has_kickers'] else 'No'}")
            click.echo(f"Has Defense: {'Yes' if league_info['has_defense'] else 'No'}")
            click.echo(f"Total QB Slots: {league_info['qb_slots']}")
            click.echo(f"Detection Source: {league_info['detection_source']}")
            
            if league_info['has_qb_flex']:
                click.echo("\n⚡ QB-Flex League Detected:")
                click.echo("  • QB values adjusted for scarcity")
                click.echo("  • Enhanced consistency scoring")
                click.echo("  • Boom/bust thresholds optimized")
        
        # Show position summary
        df = runner.run_league_aware_query("position_summary")
        click.echo("\n📊 Position Summary:")
        click.echo("-" * 30)
        output = runner.format_results(df, format="table")
        click.echo(output)
        
    except Exception as e:
        click.echo(f"❌ League configuration error: {e}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--league-id', help='League ID for auto-detection')
def draft(league_id: str):
    """Start the interactive league-aware draft tool."""
    try:
        import subprocess
        import sys
        
        # Run the draft tool as a module
        cmd = [sys.executable, '-m', 'src.draft.main']
        
        if league_id:
            click.echo(f"🏈 Starting draft tool for league {league_id}")
        else:
            click.echo("🏈 Starting draft tool with default settings")
            
        # Execute the draft tool
        subprocess.run(cmd, check=True)
        
    except KeyboardInterrupt:
        click.echo("\n👋 Draft session ended by user")
    except subprocess.CalledProcessError as e:
        click.echo(f"❌ Draft tool error: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"❌ Failed to start draft tool: {e}", err=True)
        sys.exit(1)

if __name__ == "__main__":
    cli()