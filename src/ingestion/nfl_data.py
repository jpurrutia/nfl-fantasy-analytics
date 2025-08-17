#!/usr/bin/env python3
"""NFL data ingestion module for loading data from nfl-data-py to DuckDB."""

import nfl_data_py as nfl
import duckdb
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging
from typing import List, Optional
from tqdm import tqdm

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
    
    def load_snap_counts(self, years: List[int]) -> int:
        """
        Load snap count data to bronze.nfl_snap_counts table.
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading snap count data for years: {years}")
        
        try:
            # Import snap count data
            snap_data = nfl.import_snap_counts(years=years)
            
            if snap_data.empty:
                logger.warning("No snap count data returned")
                return 0
            
            # Clean the data - remove problematic columns
            if '.progress' in snap_data.columns:
                snap_data = snap_data.drop(columns=['.progress'])
            
            snap_data = snap_data.replace('', pd.NA)  # Empty strings to pandas NA
            snap_data = snap_data.replace({float('inf'): pd.NA, float('-inf'): pd.NA})
            
            # Clear existing data for these years
            years_str = ','.join(str(year) for year in years)
            self.conn.execute(f"DELETE FROM bronze.nfl_snap_counts WHERE season IN ({years_str})")
            
            record_count = len(snap_data)
            
            # Get column names from the dataframe (excluding created_at which is auto-generated)
            data_columns = list(snap_data.columns)
            columns_str = ', '.join(data_columns)
            
            self.conn.execute(f"""
                INSERT INTO bronze.nfl_snap_counts ({columns_str})
                SELECT * FROM snap_data
            """)
            
            logger.info(f"✓ Loaded {record_count} snap count records to bronze.nfl_snap_counts")
            return record_count
            
        except Exception as e:
            logger.error(f"Failed to load snap counts: {e}")
            raise

    def load_ngs_passing(self, years: List[int]) -> int:
        """
        Load NGS passing data to bronze.nfl_ngs_passing table.
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading NGS passing data for years: {years}")
        
        try:
            # Import NGS passing data
            ngs_data = nfl.import_ngs_data('passing', years=years)
            
            if ngs_data.empty:
                logger.warning("No NGS passing data returned")
                return 0
            
            # Clean the data - handle empty strings and inf values
            ngs_data = ngs_data.replace('', pd.NA)  # Empty strings to pandas NA
            ngs_data = ngs_data.replace({float('inf'): pd.NA, float('-inf'): pd.NA})
            
            # Clear existing data for these years
            years_str = ','.join(str(year) for year in years)
            self.conn.execute(f"DELETE FROM bronze.nfl_ngs_passing WHERE season IN ({years_str})")
            
            record_count = len(ngs_data)
            
            # Get column names from the dataframe (excluding created_at which is auto-generated)
            data_columns = list(ngs_data.columns)
            columns_str = ', '.join(data_columns)
            
            self.conn.execute(f"""
                INSERT INTO bronze.nfl_ngs_passing ({columns_str})
                SELECT * FROM ngs_data
            """)
            
            logger.info(f"✓ Loaded {record_count} NGS passing records to bronze.nfl_ngs_passing")
            return record_count
            
        except Exception as e:
            logger.error(f"Failed to load NGS passing data: {e}")
            raise

    def load_ngs_rushing(self, years: List[int]) -> int:
        """
        Load NGS rushing data to bronze.nfl_ngs_rushing table.
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading NGS rushing data for years: {years}")
        
        try:
            # Import NGS rushing data
            ngs_data = nfl.import_ngs_data('rushing', years=years)
            
            if ngs_data.empty:
                logger.warning("No NGS rushing data returned")
                return 0
            
            # Clean the data - handle empty strings and inf values
            ngs_data = ngs_data.replace('', pd.NA)  # Empty strings to pandas NA
            ngs_data = ngs_data.replace({float('inf'): pd.NA, float('-inf'): pd.NA})
            
            # Clear existing data for these years
            years_str = ','.join(str(year) for year in years)
            self.conn.execute(f"DELETE FROM bronze.nfl_ngs_rushing WHERE season IN ({years_str})")
            
            record_count = len(ngs_data)
            
            # Get column names from the dataframe (excluding created_at which is auto-generated)
            data_columns = list(ngs_data.columns)
            columns_str = ', '.join(data_columns)
            
            self.conn.execute(f"""
                INSERT INTO bronze.nfl_ngs_rushing ({columns_str})
                SELECT * FROM ngs_data
            """)
            
            logger.info(f"✓ Loaded {record_count} NGS rushing records to bronze.nfl_ngs_rushing")
            return record_count
            
        except Exception as e:
            logger.error(f"Failed to load NGS rushing data: {e}")
            raise

    def load_ngs_receiving(self, years: List[int]) -> int:
        """
        Load NGS receiving data to bronze.nfl_ngs_receiving table.
        
        Args:
            years: List of years to load data for
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading NGS receiving data for years: {years}")
        
        try:
            # Import NGS receiving data
            ngs_data = nfl.import_ngs_data('receiving', years=years)
            
            if ngs_data.empty:
                logger.warning("No NGS receiving data returned")
                return 0
            
            # Clean the data - handle empty strings and inf values
            ngs_data = ngs_data.replace('', pd.NA)  # Empty strings to pandas NA
            ngs_data = ngs_data.replace({float('inf'): pd.NA, float('-inf'): pd.NA})
            
            # Clear existing data for these years
            years_str = ','.join(str(year) for year in years)
            self.conn.execute(f"DELETE FROM bronze.nfl_ngs_receiving WHERE season IN ({years_str})")
            
            record_count = len(ngs_data)
            
            # Get column names from the dataframe (excluding created_at which is auto-generated)
            data_columns = list(ngs_data.columns)
            columns_str = ', '.join(data_columns)
            
            self.conn.execute(f"""
                INSERT INTO bronze.nfl_ngs_receiving ({columns_str})
                SELECT * FROM ngs_data
            """)
            
            logger.info(f"✓ Loaded {record_count} NGS receiving records to bronze.nfl_ngs_receiving")
            return record_count
            
        except Exception as e:
            logger.error(f"Failed to load NGS receiving data: {e}")
            raise
    
    def load_play_by_play(self, years: List[int], chunk_size: int = 10000) -> int:
        """
        Load play-by-play data to bronze.nfl_play_by_play table.
        
        Args:
            years: List of years to load data for
            chunk_size: Number of rows to process at once
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading play-by-play data for years: {years}")
        logger.info("This may take several minutes per season...")
        
        total_records = 0
        
        for year in years:
            try:
                logger.info(f"Loading {year} season...")
                
                # Load pbp data for the year
                pbp = nfl.import_pbp_data(
                    years=[year], 
                    columns=None,  # Get all columns
                    downcast=False,  # Keep original types
                    include_participation=False  # Skip participation for now
                )
                
                # Select columns that match our schema
                columns_to_keep = [
                    # Game identifiers
                    'game_id', 'play_id', 'drive', 'season', 'week', 
                    'season_type', 'game_date', 'start_time',
                    # Teams
                    'home_team', 'away_team', 'posteam', 'defteam', 'posteam_type',
                    # Game situation
                    'qtr', 'quarter_seconds_remaining', 'game_seconds_remaining',
                    'half_seconds_remaining', 'game_half', 'drive_start_yard_line',
                    'drive_end_yard_line',
                    # Play details
                    'down', 'ydstogo', 'yardline_100', 'side_of_field', 'goal_to_go',
                    'play_type', 'play_type_nfl',
                    # Formation
                    'shotgun', 'no_huddle', 'qb_dropback', 'qb_scramble',
                    # Play outcome
                    'yards_gained', 'yards_after_catch', 'air_yards', 'first_down',
                    'touchdown', 'pass_touchdown', 'rush_touchdown', 'return_touchdown',
                    # Passing
                    'pass', 'pass_attempt', 'complete_pass', 'incomplete_pass',
                    'passing_yards', 'passer_player_id', 'passer_player_name',
                    'receiver_player_id', 'receiver_player_name', 'pass_length',
                    'pass_location', 'interception',
                    # Rushing
                    'rush', 'rush_attempt', 'rushing_yards', 'rusher_player_id',
                    'rusher_player_name', 'run_location', 'run_gap',
                    # Scoring
                    'td_player_id', 'td_player_name', 'td_team', 'two_point_attempt',
                    'two_point_conv_result', 'extra_point_attempt', 'extra_point_result',
                    'field_goal_attempt', 'field_goal_result', 'kick_distance',
                    # Turnovers
                    'fumble', 'fumble_lost', 'fumble_recovery_1_player_id',
                    'fumble_recovery_1_team',
                    # Penalties
                    'penalty', 'penalty_type', 'penalty_yards', 'penalty_team',
                    # Advanced metrics
                    'epa', 'wp', 'wpa', 'success', 'cpoe',
                    'air_epa', 'yac_epa', 'comp_air_epa', 'comp_yac_epa',
                    'total_home_epa', 'total_away_epa',
                    # Win probability
                    'vegas_wp', 'vegas_home_wp', 'home_wp', 'away_wp',
                    # Scoring probabilities
                    'td_prob', 'fg_prob', 'safety_prob', 'no_score_prob',
                    # Fantasy
                    'fantasy', 'fantasy_player_id', 'fantasy_player_name',
                    # Special teams
                    'special_teams_play', 'st_play_type', 'kickoff_attempt',
                    'punt_attempt', 'return_yards',
                    # Sacks
                    'sack', 'sack_player_id', 'sack_player_name', 'qb_hit',
                    # Score state
                    'score_differential', 'score_differential_post', 'posteam_score',
                    'defteam_score', 'total_home_score', 'total_away_score'
                ]
                
                # Keep only columns that exist in the dataframe
                available_cols = [col for col in columns_to_keep if col in pbp.columns]
                pbp_filtered = pbp[available_cols].copy()
                
                # Add the 'desc' column as 'play_desc' to avoid SQL reserved word conflict
                if 'desc' in pbp.columns:
                    pbp_filtered['play_desc'] = pbp['desc']
                
                # Add created_at timestamp
                pbp_filtered['created_at'] = datetime.now()
                
                # Remove existing data for this season
                self.conn.execute(f"DELETE FROM bronze.nfl_play_by_play WHERE season = {year}")
                
                # Get the column order from the target table
                table_columns = self.conn.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'bronze' 
                    AND table_name = 'nfl_play_by_play'
                    AND column_name != 'created_at'
                    ORDER BY ordinal_position
                """).fetchall()
                table_columns = [col[0] for col in table_columns]
                
                # Ensure all required columns exist, add None for missing columns
                for col in table_columns:
                    if col not in pbp_filtered.columns:
                        pbp_filtered[col] = None
                
                # Add created_at if not already present
                if 'created_at' not in table_columns:
                    table_columns.append('created_at')
                
                # Reorder columns to match table
                pbp_filtered = pbp_filtered[table_columns]
                
                # Load data in chunks for better memory management
                num_chunks = len(pbp_filtered) // chunk_size + 1
                
                for i in tqdm(range(0, len(pbp_filtered), chunk_size), 
                             desc=f"Loading {year} plays", 
                             total=num_chunks):
                    chunk = pbp_filtered.iloc[i:i+chunk_size]
                    
                    # Insert with explicit column order
                    columns_str = ', '.join(table_columns)
                    self.conn.execute(f"""
                        INSERT INTO bronze.nfl_play_by_play ({columns_str})
                        SELECT * FROM chunk
                    """)
                
                year_records = len(pbp_filtered)
                total_records += year_records
                logger.info(f"✓ Loaded {year_records:,} plays for {year} season")
                
            except Exception as e:
                logger.error(f"✗ Error loading play-by-play data for {year}: {e}")
                logger.info("Continuing with next year...")
                continue
        
        logger.info(f"✓ Total plays loaded: {total_records:,}")
        return total_records
    
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
    
    # Define years to load (start with 2023, add 2024 when available)
    years = [2023]
    
    logger.info("=" * 50)
    logger.info("Starting NFL Data Ingestion")
    logger.info("=" * 50)
    
    # Load core data first
    try:
        player_count = ingestion.load_players(years)
    except Exception as e:
        logger.error(f"Failed to load players: {e}")
        return
    
    try:
        perf_count = ingestion.load_player_performance(years)
    except Exception as e:
        logger.error(f"Failed to load performance data: {e}")
        return
    
    # Load advanced metrics data
    try:
        snap_count = ingestion.load_snap_counts(years)
        logger.info(f"✓ Loaded {snap_count} snap count records")
    except Exception as e:
        logger.error(f"Failed to load snap counts: {e}")
        # Continue with other data sources
    
    try:
        ngs_passing_count = ingestion.load_ngs_passing(years)
        logger.info(f"✓ Loaded {ngs_passing_count} NGS passing records")
    except Exception as e:
        logger.error(f"Failed to load NGS passing data: {e}")
    
    try:
        ngs_rushing_count = ingestion.load_ngs_rushing(years)
        logger.info(f"✓ Loaded {ngs_rushing_count} NGS rushing records")
    except Exception as e:
        logger.error(f"Failed to load NGS rushing data: {e}")
    
    try:
        ngs_receiving_count = ingestion.load_ngs_receiving(years)
        logger.info(f"✓ Loaded {ngs_receiving_count} NGS receiving records")
    except Exception as e:
        logger.error(f"Failed to load NGS receiving data: {e}")
    
    # Load play-by-play data (this is large and takes time)
    try:
        pbp_count = ingestion.load_play_by_play(years)
        logger.info(f"✓ Loaded {pbp_count} play-by-play records")
    except Exception as e:
        logger.error(f"Failed to load play-by-play data: {e}")
    
    # Run validation
    validation_results = ingestion.validate_data()
    
    logger.info("=" * 50)
    logger.info("Ingestion Complete!")
    logger.info(f"Total players: {validation_results['player_count']}")
    logger.info(f"Total performance records: {validation_results['performance_count']}")
    logger.info("=" * 50)

if __name__ == "__main__":
    main()