#!/usr/bin/env python3
"""Player mapping module for linking ESPN and NFL data sources."""

import duckdb
import pandas as pd
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlayerMapper:
    """Maps players between ESPN and NFL data sources using fuzzy matching."""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        """Initialize with database connection."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        logger.info(f"Connected to database: {db_path}")
        
        # Create mapping table if it doesn't exist
        self._create_mapping_table()
    
    def __del__(self):
        """Close database connection on cleanup."""
        if hasattr(self, 'conn'):
            self.conn.close()
    
    def _create_mapping_table(self):
        """Create the player mapping table if it doesn't exist."""
        # The table already exists with a different schema - let's work with what we have
        # The existing schema uses: universal_player_id, platform, platform_player_id, player_name, player_name_variant
        logger.info("Using existing player mapping table structure")
    
    def normalize_name(self, name: str) -> str:
        """
        Normalize player name for matching.
        
        Args:
            name: Player name to normalize
            
        Returns:
            Normalized name
        """
        if not name:
            return ""
        
        # Convert to lowercase
        name = name.lower()
        
        # Remove common suffixes
        suffixes = [' jr.', ' jr', ' iii', ' ii', ' iv', ' sr.', ' sr']
        for suffix in suffixes:
            name = name.replace(suffix, '')
        
        # Remove punctuation
        name = re.sub(r'[^\w\s]', '', name)
        
        # Remove extra spaces
        name = ' '.join(name.split())
        
        return name
    
    def calculate_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity score between two names.
        
        Args:
            name1: First name
            name2: Second name
            
        Returns:
            Similarity score between 0 and 1
        """
        norm1 = self.normalize_name(name1)
        norm2 = self.normalize_name(name2)
        
        # Use SequenceMatcher for fuzzy matching
        return SequenceMatcher(None, norm1, norm2).ratio()
    
    def map_espn_to_nfl(self, espn_players: pd.DataFrame) -> pd.DataFrame:
        """
        Map ESPN players to NFL players.
        
        Args:
            espn_players: DataFrame with ESPN player data (must have 'name', 'position', 'team' columns)
            
        Returns:
            DataFrame with mapping results
        """
        logger.info(f"Mapping {len(espn_players)} ESPN players to NFL data...")
        
        # Get NFL players from database (include RES for reserve players)
        nfl_players = self.conn.execute("""
            SELECT player_id, name, position, team 
            FROM bronze.nfl_players
            WHERE status IN ('ACT', 'RES')
        """).fetchdf()
        
        mappings = []
        
        for _, espn_player in espn_players.iterrows():
            espn_name = espn_player['name']
            espn_pos = espn_player.get('position', '')
            espn_team = espn_player.get('team', '')
            
            # First try exact match
            exact_matches = nfl_players[
                nfl_players['name'].str.lower() == espn_name.lower()
            ]
            
            if len(exact_matches) == 1:
                # Perfect match
                nfl_player = exact_matches.iloc[0]
                universal_id = nfl_player['player_id']
                
                # Add ESPN mapping (universal_player_id is the NFL player ID)
                mappings.append({
                    'universal_player_id': universal_id,
                    'platform': 'ESPN',
                    'platform_player_id': espn_player.get('id', espn_name),
                    'player_name': espn_name,
                    'player_name_variant': nfl_player['name'],
                    'position': nfl_player['position'],
                    'team': nfl_player['team'],
                    'confidence_score': 1.0,
                    'mapping_method': 'exact'
                })
            else:
                # Try fuzzy matching
                best_match = None
                best_score = 0
                
                # Filter NFL players by position if available
                candidates = nfl_players
                if espn_pos:
                    # Map ESPN positions to NFL positions
                    pos_map = {
                        'QB': ['QB'],
                        'RB': ['RB', 'FB'],
                        'WR': ['WR'],
                        'TE': ['TE'],
                        'DST': [],  # Skip team defenses
                        'K': ['K'],
                        'D/ST': []  # Skip team defenses
                    }
                    
                    if espn_pos in pos_map and pos_map[espn_pos]:
                        candidates = nfl_players[
                            nfl_players['position'].isin(pos_map[espn_pos])
                        ]
                
                for _, nfl_player in candidates.iterrows():
                    score = self.calculate_similarity(espn_name, nfl_player['name'])
                    
                    # Boost score if team matches
                    if espn_team and nfl_player['team'] == espn_team:
                        score += 0.1
                    
                    if score > best_score:
                        best_score = score
                        best_match = nfl_player
                
                # Only accept matches above threshold
                if best_match is not None and best_score > 0.8:
                    # Create a universal ID from the NFL player ID
                    universal_id = best_match['player_id']
                    mappings.append({
                        'universal_player_id': universal_id,
                        'platform': 'ESPN',
                        'platform_player_id': espn_player.get('id', espn_name),
                        'player_name': espn_name,
                        'player_name_variant': best_match['name'],
                        'position': best_match['position'],
                        'team': best_match['team'],
                        'confidence_score': round(best_score, 2),
                        'mapping_method': 'fuzzy'
                    })
                else:
                    # No good match found
                    logger.warning(f"No match found for ESPN player: {espn_name} ({espn_pos})")
        
        result_df = pd.DataFrame(mappings)
        logger.info(f"✓ Mapped {len(result_df)} out of {len(espn_players)} players")
        
        return result_df
    
    def save_mappings(self, mappings: pd.DataFrame):
        """
        Save player mappings to database.
        
        Args:
            mappings: DataFrame with mapping data
        """
        if mappings.empty:
            logger.warning("No mappings to save")
            return
        
        # Filter to only the columns that exist in the table
        table_columns = ['universal_player_id', 'platform', 'platform_player_id', 
                        'player_name', 'player_name_variant', 'position', 'team']
        mappings_to_save = mappings[table_columns].copy()
        mappings_to_save['created_at'] = pd.Timestamp.now()
        
        # Clear existing mappings for all universal player IDs we're about to insert
        universal_ids = mappings_to_save['universal_player_id'].unique()
        if len(universal_ids) > 0:
            placeholders = ','.join([f"'{id}'" for id in universal_ids])
            self.conn.execute(
                f"DELETE FROM bronze.player_id_mapping WHERE universal_player_id IN ({placeholders})"
            )
        
        # Insert new mappings
        self.conn.execute("""
            INSERT INTO bronze.player_id_mapping 
            SELECT * FROM mappings_to_save
        """)
        
        logger.info(f"✓ Saved {len(mappings_to_save)} player mappings to database")
    
    def get_mapping_stats(self) -> Dict:
        """
        Get statistics about player mappings.
        
        Returns:
            Dictionary with mapping statistics
        """
        stats = {}
        
        # Total mappings
        total = self.conn.execute(
            "SELECT COUNT(*) FROM bronze.player_id_mapping"
        ).fetchone()[0]
        stats['total_mappings'] = total
        
        # By platform
        by_platform = self.conn.execute("""
            SELECT platform, COUNT(*) as count
            FROM bronze.player_id_mapping
            GROUP BY platform
        """).fetchdf()
        stats['by_platform'] = by_platform.to_dict('records')
        
        # Unique universal players
        unique_players = self.conn.execute("""
            SELECT COUNT(DISTINCT universal_player_id) as unique_count
            FROM bronze.player_id_mapping
        """).fetchone()[0]
        stats['unique_players'] = unique_players
        
        return stats

def test_mapping():
    """Test the player mapping with sample ESPN data."""
    
    mapper = PlayerMapper()
    
    # Create sample ESPN players for testing
    sample_espn_players = pd.DataFrame([
        {'id': '1', 'name': 'Josh Allen', 'position': 'QB', 'team': 'BUF'},
        {'id': '2', 'name': 'Christian McCaffrey', 'position': 'RB', 'team': 'SF'},
        {'id': '3', 'name': 'Justin Jefferson', 'position': 'WR', 'team': 'MIN'},
        {'id': '4', 'name': 'Travis Kelce', 'position': 'TE', 'team': 'KC'},
        {'id': '5', 'name': 'Patrick Mahomes', 'position': 'QB', 'team': 'KC'},
        {'id': '6', 'name': 'Tyreek Hill', 'position': 'WR', 'team': 'MIA'},
        {'id': '7', 'name': 'A.J. Brown', 'position': 'WR', 'team': 'PHI'},
        {'id': '8', 'name': 'Derrick Henry', 'position': 'RB', 'team': 'BAL'},
        {'id': '9', 'name': 'CeeDee Lamb', 'position': 'WR', 'team': 'DAL'},
        {'id': '10', 'name': 'Dak Prescott', 'position': 'QB', 'team': 'DAL'}
    ])
    
    logger.info("=" * 50)
    logger.info("Testing Player Mapping")
    logger.info("=" * 50)
    
    # Perform mapping
    mappings = mapper.map_espn_to_nfl(sample_espn_players)
    
    if not mappings.empty:
        # Display results
        print("\nMapping Results:")
        espn_mappings = mappings[mappings['platform'] == 'ESPN']
        print(espn_mappings[['player_name', 'player_name_variant', 'position', 'team']])
        
        # Save mappings
        mapper.save_mappings(mappings)
        
        # Get stats
        stats = mapper.get_mapping_stats()
        print("\nMapping Statistics:")
        print(f"  Total mappings: {stats['total_mappings']}")
        print(f"  Unique players: {stats['unique_players']}")
        
        for platform_stat in stats['by_platform']:
            print(f"  {platform_stat['platform']}: {platform_stat['count']} mappings")
    
    logger.info("=" * 50)
    logger.info("Mapping test complete!")

if __name__ == "__main__":
    test_mapping()