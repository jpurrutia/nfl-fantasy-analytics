"""
ESPN League Data Synchronization

Pulls complete league data from ESPN including:
- League settings and configuration
- Team/manager information  
- Current rosters for all teams
- Player ownership and draft data
"""

import duckdb
import json
from typing import Dict, List, Optional, Any
from pathlib import Path
from datetime import datetime
import logging
import pandas as pd

from src.connectors.espn_api import ESPNConnector
from src.utils.league_config import LeagueConfig, ESPNLeagueDetector

logger = logging.getLogger(__name__)


class ESPNLeagueSync:
    """Synchronize ESPN league data to DuckDB."""
    
    def __init__(self, db_path: str = "data/nfl_analytics.duckdb"):
        """Initialize with database connection."""
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        self._ensure_tables()
    
    def _ensure_tables(self):
        """Create tables if they don't exist."""
        self.conn.execute("""
            CREATE SCHEMA IF NOT EXISTS bronze;
            
            -- League metadata
            CREATE TABLE IF NOT EXISTS bronze.espn_leagues (
                league_id VARCHAR PRIMARY KEY,
                league_name VARCHAR,
                season INTEGER,
                num_teams INTEGER,
                scoring_type VARCHAR,
                roster_settings JSON,
                scoring_settings JSON,
                updated_at TIMESTAMP DEFAULT NOW()
            );
            
            -- Team/Manager information
            CREATE TABLE IF NOT EXISTS bronze.espn_teams (
                team_id VARCHAR,
                league_id VARCHAR,
                season INTEGER,
                team_name VARCHAR,
                team_abbrev VARCHAR,
                manager_name VARCHAR,
                manager_id VARCHAR,
                draft_position INTEGER,
                current_rank INTEGER,
                points_for DECIMAL(10,2),
                points_against DECIMAL(10,2),
                wins INTEGER,
                losses INTEGER,
                ties INTEGER,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (team_id, league_id, season)
            );
            
            -- Current rosters
            CREATE TABLE IF NOT EXISTS bronze.espn_rosters (
                league_id VARCHAR,
                season INTEGER,
                team_id VARCHAR,
                player_id VARCHAR,
                player_name VARCHAR,
                position VARCHAR,
                pro_team VARCHAR,
                roster_slot VARCHAR,  -- Starting lineup slot or BENCH
                acquisition_type VARCHAR,  -- DRAFT, ADD, TRADE
                acquisition_date BIGINT,  -- ESPN sends as timestamp
                draft_round INTEGER,
                draft_pick INTEGER,
                keeper_status BOOLEAN DEFAULT FALSE,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (league_id, season, team_id, player_id)
            );
            
            -- Player ownership across leagues
            CREATE TABLE IF NOT EXISTS bronze.espn_player_ownership (
                player_id VARCHAR,
                player_name VARCHAR,
                position VARCHAR,
                pro_team VARCHAR,
                ownership_pct DECIMAL(5,2),  -- Across all ESPN leagues
                start_pct DECIMAL(5,2),      -- % started this week
                projected_points DECIMAL(6,2),
                actual_points DECIMAL(6,2),
                week INTEGER,
                season INTEGER,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (player_id, week, season)
            );
        """)
        logger.info("✅ ESPN league tables initialized")
    
    def sync_league(self, league_id: int, year: int = 2025) -> Dict[str, Any]:
        """
        Sync all data for a specific league.
        
        Args:
            league_id: ESPN league ID
            year: Season year
            
        Returns:
            Summary of synced data
        """
        try:
            # Load credentials from config
            import yaml
            from pathlib import Path
            
            config_path = Path("config/config.yaml")
            if config_path.exists():
                with open(config_path) as f:
                    config = yaml.safe_load(f)
                espn_config = config.get("espn", {})
                swid = espn_config.get("swid")
                espn_s2 = espn_config.get("espn_s2")
            else:
                swid = None
                espn_s2 = None
            
            # Create ESPN connector with authentication
            connector = ESPNConnector(
                league_id=league_id, 
                year=year,
                swid=swid,
                espn_s2=espn_s2
            )
            
            # Test connection
            if not connector.test_connection():
                logger.error(f"Failed to connect to ESPN league {league_id}")
                return {"error": "Connection failed"}
            
            # Get league settings
            settings = connector.get_league_settings()
            if not settings:
                logger.error("Failed to fetch league settings")
                return {"error": "No settings found"}
            
            # Store league metadata
            self._store_league_settings(league_id, settings, year)
            
            # Get and store teams
            teams = connector.get_teams()
            team_count = self._store_teams(league_id, teams, year)
            
            # Get and store rosters for each team
            roster_count = 0
            for team in teams:
                team_id = team.get("id")
                if team_id:
                    roster = connector.get_roster(team_id)
                    roster_count += self._store_roster(league_id, team_id, roster, year)
            
            # Get current player pool with ownership
            players = connector.get_players()
            player_count = self._store_player_ownership(players, year)
            
            summary = {
                "league_id": league_id,
                "league_name": settings.name,
                "season": year,
                "teams_synced": team_count,
                "players_on_rosters": roster_count,
                "player_pool_size": player_count,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"✅ League sync complete: {summary}")
            return summary
            
        except Exception as e:
            logger.error(f"League sync failed: {e}")
            return {"error": str(e)}
    
    def _store_league_settings(self, league_id: int, settings: Any, year: int):
        """Store league settings in database."""
        self.conn.execute("""
            INSERT INTO bronze.espn_leagues (
                league_id, league_name, season, num_teams,
                scoring_type, roster_settings, scoring_settings
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (league_id) DO UPDATE SET
                league_name = EXCLUDED.league_name,
                num_teams = EXCLUDED.num_teams,
                scoring_type = EXCLUDED.scoring_type,
                roster_settings = EXCLUDED.roster_settings,
                scoring_settings = EXCLUDED.scoring_settings,
                updated_at = NOW()
        """, [
            str(league_id),
            settings.name,
            year,
            settings.num_teams,
            settings.scoring_type,
            json.dumps(settings.roster_slots),
            json.dumps(settings.scoring_details)
        ])
        logger.info(f"✅ Stored settings for {settings.name}")
    
    def _store_teams(self, league_id: int, teams: List[Dict], year: int) -> int:
        """Store team/manager information."""
        count = 0
        for team in teams:
            # Extract team data
            team_id = str(team.get("id", ""))
            team_name = team.get("name", "Unknown Team")
            team_abbrev = team.get("abbrev", "")
            
            # Extract manager info from owners array
            owners = team.get("owners", [])
            manager_name = "Unknown Manager"
            manager_id = ""
            if owners:
                # ESPN stores manager info in members endpoint
                manager_id = owners[0] if isinstance(owners[0], str) else ""
            
            # Get team record
            record = team.get("record", {}).get("overall", {})
            wins = record.get("wins", 0)
            losses = record.get("losses", 0) 
            ties = record.get("ties", 0)
            points_for = record.get("pointsFor", 0)
            points_against = record.get("pointsAgainst", 0)
            
            # Get draft position and current rank
            draft_position = team.get("draftDayProjectedRank", 0)
            current_rank = team.get("currentProjectedRank", 0)
            
            self.conn.execute("""
                INSERT INTO bronze.espn_teams (
                    team_id, league_id, season, team_name, team_abbrev,
                    manager_name, manager_id, draft_position, current_rank,
                    points_for, points_against, wins, losses, ties
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (team_id, league_id, season) DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    current_rank = EXCLUDED.current_rank,
                    points_for = EXCLUDED.points_for,
                    points_against = EXCLUDED.points_against,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    ties = EXCLUDED.ties,
                    updated_at = NOW()
            """, [
                team_id, str(league_id), year, team_name, team_abbrev,
                manager_name, manager_id, draft_position, current_rank,
                points_for, points_against, wins, losses, ties
            ])
            count += 1
        
        logger.info(f"✅ Stored {count} teams")
        return count
    
    def _store_roster(self, league_id: int, team_id: int, roster: List[Dict], year: int) -> int:
        """Store roster for a specific team."""
        count = 0
        for player_entry in roster:
            try:
                # Extract player info
                player_info = player_entry.get("playerPoolEntry", {}).get("player", {})
                if not player_info:
                    continue
                
                player_id = str(player_info.get("id", ""))
                player_name = player_info.get("fullName", "Unknown")
                
                # Map position ID to position name
                position_id = player_info.get("defaultPositionId", 0)
                position = self._map_position(position_id)
                
                # Get pro team
                pro_team_id = player_info.get("proTeamId", 0)
                pro_team = self._map_team(pro_team_id)
                
                # Get lineup slot
                lineup_slot_id = player_entry.get("lineupSlotId", 20)  # 20 = BENCH
                roster_slot = self._map_lineup_slot(lineup_slot_id)
                
                # Get acquisition info
                acquisition_type = player_entry.get("acquisitionType", "DRAFT")
                acquisition_date = player_entry.get("acquisitionDate")
                # Convert acquisition_date to NULL if not present or handle timestamp
                if acquisition_date is None or acquisition_date == 0:
                    acquisition_date = None
                
                # Store in database
                self.conn.execute("""
                    INSERT INTO bronze.espn_rosters (
                        league_id, season, team_id, player_id, player_name,
                        position, pro_team, roster_slot, acquisition_type,
                        acquisition_date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (league_id, season, team_id, player_id) DO UPDATE SET
                        roster_slot = EXCLUDED.roster_slot,
                        updated_at = NOW()
                """, [
                    str(league_id), year, str(team_id), player_id, player_name,
                    position, pro_team, roster_slot, acquisition_type,
                    acquisition_date
                ])
                count += 1
                
            except Exception as e:
                logger.warning(f"Failed to store player: {e}")
                continue
        
        return count
    
    def _store_player_ownership(self, players: List, year: int) -> int:
        """Store player ownership and projection data."""
        # This would need enhancement to get ownership data
        # ESPN API requires additional endpoints for this
        return len(players) if players else 0
    
    def _map_position(self, position_id: int) -> str:
        """Map ESPN position ID to position name."""
        position_map = {
            1: "QB", 2: "RB", 3: "WR", 4: "TE",
            5: "K", 16: "D/ST"
        }
        return position_map.get(position_id, "UNKNOWN")
    
    def _map_team(self, team_id: int) -> str:
        """Map ESPN team ID to team abbreviation."""
        # This would need a complete mapping
        # For now, return the ID as string
        return f"TEAM_{team_id}"
    
    def _map_lineup_slot(self, slot_id: int) -> str:
        """Map lineup slot ID to slot name."""
        slot_map = {
            0: "QB", 2: "RB", 4: "WR", 6: "TE",
            7: "K", 16: "D/ST", 17: "K",
            20: "BENCH", 21: "IR", 23: "FLEX"
        }
        return slot_map.get(slot_id, "BENCH")
    
    def get_league_summary(self, league_id: int) -> pd.DataFrame:
        """
        Get a summary of league data.
        
        Returns:
            DataFrame with team standings and roster composition
        """
        query = """
            SELECT 
                t.team_name,
                t.manager_name,
                t.wins,
                t.losses,
                t.points_for,
                COUNT(r.player_id) as roster_size,
                SUM(CASE WHEN r.position = 'QB' THEN 1 ELSE 0 END) as qb_count,
                SUM(CASE WHEN r.position = 'RB' THEN 1 ELSE 0 END) as rb_count,
                SUM(CASE WHEN r.position = 'WR' THEN 1 ELSE 0 END) as wr_count,
                SUM(CASE WHEN r.position = 'TE' THEN 1 ELSE 0 END) as te_count
            FROM bronze.espn_teams t
            LEFT JOIN bronze.espn_rosters r 
                ON t.team_id = r.team_id 
                AND t.league_id = r.league_id
            WHERE t.league_id = ?
            GROUP BY t.team_id, t.team_name, t.manager_name, 
                     t.wins, t.losses, t.points_for
            ORDER BY t.wins DESC, t.points_for DESC
        """
        
        return self.conn.execute(query, [str(league_id)]).df()
    
    def get_roster_analysis(self, league_id: int) -> pd.DataFrame:
        """
        Analyze roster composition across the league.
        
        Returns:
            DataFrame with player distribution analysis
        """
        query = """
            SELECT 
                player_name,
                position,
                COUNT(*) as rostered_count,
                STRING_AGG(DISTINCT team_id, ', ') as on_teams
            FROM bronze.espn_rosters
            WHERE league_id = ?
            GROUP BY player_name, position
            HAVING COUNT(*) > 1
            ORDER BY rostered_count DESC
        """
        
        return self.conn.execute(query, [str(league_id)]).df()


if __name__ == "__main__":
    # Test with sample league
    import yaml
    from pathlib import Path
    
    # Load config
    config_path = Path("config/config.yaml")
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        
        espn_config = config.get("espn", {})
        league_id = espn_config.get("league_id")
        
        if league_id:
            # Initialize sync
            sync = ESPNLeagueSync()
            
            # Sync league data
            print(f"🔄 Syncing league {league_id}...")
            result = sync.sync_league(league_id)
            print(f"✅ Sync result: {result}")
            
            # Show summary
            if "error" not in result:
                print("\n📊 League Summary:")
                summary = sync.get_league_summary(league_id)
                print(summary)