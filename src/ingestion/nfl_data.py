#!/usr/bin/env python3
"""NFL data ingestion module for loading data from nfl-data-py to DuckDB."""

import nfl_data_py as nfl
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from typing import List, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NFLDataIngestion:
    """Handles ingestion of NFL data from nfl-data-py to DuckDB bronze layer."""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        """Initialize with database connection."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
    
    def __del__(self):
        """Close database connection on cleanup."""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def load_players(self, years: List[int]) -> int:
        """
        Load player roster data to bronze.players table.
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading player data for years: {years}")
        
        try:
            # Import roster data
            rosters = nfl.import_seasonal_rosters(years=years)
            
            # Get unique players (deduplicate across weeks)
            players = rosters.groupby('player_id').agg({
                'player_name': 'first',
                'position': 'first',
                'team': 'last',  # Most recent team
                'status': 'last',  # Most recent status
                'birth_date': 'first',
                'college': 'first',
                'draft_number': 'first',
                'entry_year': 'first'
            }).reset_index()
            
            # Rename columns to match our schema
            players = players.rename(columns={
                'player_name': 'name',
                'draft_number': 'draft_pick',
                'entry_year': 'draft_year'
            })
            
            # Add missing columns with default values
            players['draft_round'] = None  # We'll need to derive this from draft_pick
            players['created_at'] = datetime.now()
            players['updated_at'] = datetime.now()
            
            # Select columns that match our schema
            columns = ['player_id', 'name', 'position', 'team', 'status', 
                      'birth_date', 'college', 'draft_year', 'draft_round', 
                      'draft_pick', 'created_at', 'updated_at']
            players = players[columns]
            
            # Load to database (using REPLACE to handle updates)
            self.conn.execute("DELETE FROM bronze.nfl_players")  # Clear existing data
            self.conn.execute("""
                INSERT INTO bronze.nfl_players 
                SELECT * FROM players
            """)
            
            record_count = len(players)
            logger.info(f"✓ Loaded {record_count} players to bronze.nfl_players")
            return record_count
            
        except Exception as e:
            logger.error(f"✗ Error loading players: {e}")
            raise
    
    def load_player_performance(self, years: List[int]) -> int:
        """
        Load weekly player performance data to bronze.player_performance table.
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading player performance data for years: {years}")
        
        try:
            # Import weekly data
            weekly = nfl.import_weekly_data(years=years, downcast=False)
            
            # Filter to regular season and playoffs only
            weekly = weekly[weekly['season_type'].isin(['REG', 'POST'])]
            
            # Map columns to our schema
            performance = pd.DataFrame({
                'player_id': weekly['player_id'],
                'week': weekly['week'],
                'season': weekly['season'],
                'game_date': None,  # We'll need to get this from schedules
                'opponent': weekly['opponent_team'],
                # Passing stats
                'passing_attempts': weekly['attempts'],
                'passing_completions': weekly['completions'],
                'passing_yards': weekly['passing_yards'],
                'passing_tds': weekly['passing_tds'],
                'passing_ints': weekly['interceptions'],
                # Rushing stats
                'rushing_attempts': weekly['carries'],
                'rushing_yards': weekly['rushing_yards'],
                'rushing_tds': weekly['rushing_tds'],
                # Receiving stats
                'targets': weekly['targets'],
                'receptions': weekly['receptions'],
                'receiving_yards': weekly['receiving_yards'],
                'receiving_tds': weekly['receiving_tds'],
                # Misc stats
                'fumbles_lost': weekly['rushing_fumbles_lost'].fillna(0) + weekly['receiving_fumbles_lost'].fillna(0),
                'two_point_conversions': weekly['passing_2pt_conversions'].fillna(0) + 
                                        weekly['rushing_2pt_conversions'].fillna(0) + 
                                        weekly['receiving_2pt_conversions'].fillna(0),
                # Fantasy points
                'fantasy_points_standard': weekly['fantasy_points'],
                'fantasy_points_ppr': weekly['fantasy_points_ppr'],
                'fantasy_points_half_ppr': (weekly['fantasy_points'] + weekly['fantasy_points_ppr']) / 2,
                'created_at': datetime.now()
            })
            
            # Remove rows with all null stats (non-skill position players)
            stat_columns = ['passing_yards', 'rushing_yards', 'receiving_yards', 'targets']
            performance = performance.dropna(subset=stat_columns, how='all')
            
            # Load to database
            self.conn.execute("DELETE FROM bronze.nfl_player_performance WHERE season IN (" + 
                            ','.join(map(str, years)) + ")")
            self.conn.execute("""
                INSERT INTO bronze.nfl_player_performance 
                SELECT * FROM performance
            """)
            
            record_count = len(performance)
            logger.info(f"✓ Loaded {record_count} performance records to bronze.nfl_player_performance")
            return record_count
            
        except Exception as e:
            logger.error(f"✗ Error loading performance data: {e}")
            raise
    
    def load_player_opportunity(self, years: List[int]) -> int:
        """
        Load player opportunity metrics (placeholder for now - needs additional data sources).
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info("Player opportunity data requires additional sources (snap counts, red zone data)")
        logger.info("This will be implemented when we integrate additional data sources")
        return 0
    
    def validate_data(self) -> dict:
        """
        Run basic validation checks on loaded data.
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Running data validation checks...")
        
        results = {}
        
        # Check player count
        player_count = self.conn.execute("SELECT COUNT(*) FROM bronze.nfl_players").fetchone()[0]
        results['player_count'] = player_count
        logger.info(f"  Players loaded: {player_count}")
        
        # Check performance records
        perf_count = self.conn.execute("SELECT COUNT(*) FROM bronze.nfl_player_performance").fetchone()[0]
        results['performance_count'] = perf_count
        logger.info(f"  Performance records: {perf_count}")
        
        # Check for orphaned performance records
        orphaned = self.conn.execute("""
            SELECT COUNT(*) 
            FROM bronze.nfl_player_performance p
            WHERE NOT EXISTS (
                SELECT 1 FROM bronze.nfl_players pl 
                WHERE pl.player_id = p.player_id
            )
        """).fetchone()[0]
        results['orphaned_records'] = orphaned
        if orphaned > 0:
            logger.warning(f"  ⚠ Found {orphaned} orphaned performance records")
        else:
            logger.info(f"  ✓ No orphaned records found")
        
        # Check data freshness
        latest_week = self.conn.execute("""
            SELECT MAX(week) as latest_week, MAX(season) as season
            FROM bronze.nfl_player_performance
            WHERE season = (SELECT MAX(season) FROM bronze.nfl_player_performance)
        """).fetchone()
        results['latest_data'] = {'season': latest_week[1], 'week': latest_week[0]}
        logger.info(f"  Latest data: {latest_week[1]} Week {latest_week[0]}")
        
        return results

def main():
    """Main execution function."""
    
    # Initialize ingestion
    ingestion = NFLDataIngestion()
    
    # Define years to load (start with 2024, can add 2023 later)
    years = [2024]
    
    logger.info("=" * 50)
    logger.info("Starting NFL Data Ingestion")
    logger.info("=" * 50)
    
    # Load players
    try:
        player_count = ingestion.load_players(years)
    except Exception as e:
        logger.error(f"Failed to load players: {e}")
        return
    
    # Load performance data
    try:
        perf_count = ingestion.load_player_performance(years)
    except Exception as e:
        logger.error(f"Failed to load performance data: {e}")
        return
    
    # Run validation
    validation_results = ingestion.validate_data()
    
    logger.info("=" * 50)
    logger.info("Ingestion Complete!")
    logger.info(f"Total players: {validation_results['player_count']}")
    logger.info(f"Total performance records: {validation_results['performance_count']}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()