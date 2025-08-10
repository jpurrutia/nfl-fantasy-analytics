"""
ESPN Fantasy Football API Connector

Extracted and refactored from the existing draft wizard for reusability
across the NFL Analytics platform.
"""

import requests
import json
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class LeagueSettings:
    """ESPN League Settings"""
    name: str
    season: int
    current_week: int
    num_teams: int
    roster_slots: Dict[str, int]
    is_superflex: bool
    scoring_type: str


@dataclass
class Player:
    """ESPN Player Data"""
    player_id: str
    name: str
    position: str
    team: str
    projected_points: float = 0.0
    actual_points: float = 0.0


class ESPNConnector:
    """
    ESPN Fantasy Football API connector
    
    Handles authentication and data retrieval from ESPN's fantasy API.
    Supports both public and private leagues via cookie authentication.
    """
    
    SLOT_MAP = {
        0: "QB",
        2: "RB", 
        4: "WR",
        6: "TE",
        7: "K",
        16: "D/ST",
        17: "K",
        20: "BENCH",
        21: "OP",
        23: "FLEX",
    }
    
    def __init__(self, league_id: int, year: int, swid: str = None, espn_s2: str = None):
        """
        Initialize ESPN connector
        
        Args:
            league_id: ESPN league ID
            year: Season year
            swid: SWID cookie value for private leagues
            espn_s2: espn_s2 cookie value for private leagues
        """
        self.league_id = league_id
        self.year = year
        self.swid = swid
        self.espn_s2 = espn_s2
        
        self.base_url = (
            f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/"
            f"seasons/{year}/segments/0/leagues/{league_id}"
        )
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        
        # Add authentication cookies if provided
        if swid and espn_s2:
            # Ensure SWID has proper formatting
            formatted_swid = swid if swid.startswith("{") else f"{{{swid}}}"
            self.headers["Cookie"] = f"SWID={formatted_swid}; espn_s2={espn_s2}"
    
    def get_league_settings(self) -> Optional[LeagueSettings]:
        """
        Get comprehensive league settings
        
        Returns:
            LeagueSettings object or None if request fails
        """
        try:
            url = f"{self.base_url}?view=mSettings"
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code != 200:
                logger.warning(f"Failed to fetch league settings: {response.status_code}")
                return self._get_default_settings()
            
            data = response.json()
            settings = data.get("settings", {})
            
            # Parse roster slots
            roster_slots = {}
            if "rosterSettings" in settings:
                lineup = settings["rosterSettings"].get("lineupSlotCounts", {})
                for slot_id, count in lineup.items():
                    position = self.SLOT_MAP.get(int(slot_id))
                    if position and count > 0:
                        roster_slots[position] = count
            else:
                roster_slots = self._get_default_roster()
            
            # Determine scoring type
            scoring_type = "STANDARD"
            if "scoringSettings" in settings:
                scoring_items = settings["scoringSettings"].get("scoringItems", [])
                for item in scoring_items:
                    if item.get("statId") == 53:  # Receptions
                        points = item.get("pointsOverrides", {}).get("16", 0)
                        if points == 1.0:
                            scoring_type = "PPR"
                        elif points == 0.5:
                            scoring_type = "HALF_PPR"
            
            return LeagueSettings(
                name=settings.get("name", f"League {self.league_id}"),
                season=data.get("seasonId", self.year),
                current_week=data.get("scoringPeriodId", 1),
                num_teams=settings.get("size", 10),
                roster_slots=roster_slots,
                is_superflex=roster_slots.get("OP", 0) > 0,
                scoring_type=scoring_type
            )
            
        except Exception as e:
            logger.error(f"Error fetching league settings: {e}")
            return self._get_default_settings()
    
    def get_teams(self) -> List[Dict[str, Any]]:
        """
        Get all teams in the league
        
        Returns:
            List of team data dictionaries
        """
        try:
            url = f"{self.base_url}?view=mTeam"
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            return data.get("teams", [])
            
        except Exception as e:
            logger.error(f"Error fetching teams: {e}")
            return []
    
    def get_players(self, week: Optional[int] = None) -> List[Player]:
        """
        Get player data for a specific week
        
        Args:
            week: Week number (current week if None)
            
        Returns:
            List of Player objects
        """
        try:
            url = f"{self.base_url}?view=kona_player_info"
            if week:
                url += f"&scoringPeriodId={week}"
                
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            players = []
            
            # Extract player data from response
            for player_data in data.get("players", []):
                player_info = player_data.get("player", {})
                
                player = Player(
                    player_id=str(player_info.get("id", "")),
                    name=player_info.get("fullName", ""),
                    position=self._map_position(player_info.get("defaultPositionId", 0)),
                    team=self._get_team_abbrev(player_info.get("proTeamId", 0)),
                    projected_points=self._extract_projected_points(player_data),
                    actual_points=self._extract_actual_points(player_data)
                )
                
                if player.name:  # Only add players with valid names
                    players.append(player)
            
            return players
            
        except Exception as e:
            logger.error(f"Error fetching players: {e}")
            return []
    
    def get_roster(self, team_id: int, week: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get roster for a specific team
        
        Args:
            team_id: ESPN team ID
            week: Week number (current week if None)
            
        Returns:
            List of roster entries
        """
        try:
            url = f"{self.base_url}?view=mRoster"
            if week:
                url += f"&scoringPeriodId={week}"
                
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Find team in response
            for team in data.get("teams", []):
                if team.get("id") == team_id:
                    return team.get("roster", {}).get("entries", [])
            
            return []
            
        except Exception as e:
            logger.error(f"Error fetching roster for team {team_id}: {e}")
            return []
    
    def _get_default_settings(self) -> LeagueSettings:
        """Return default league settings for fallback"""
        return LeagueSettings(
            name=f"League {self.league_id}",
            season=self.year,
            current_week=1,
            num_teams=10,
            roster_slots=self._get_default_roster(),
            is_superflex=True,
            scoring_type="STANDARD"
        )
    
    def _get_default_roster(self) -> Dict[str, int]:
        """Return default superflex roster configuration"""
        return {
            "QB": 1,
            "RB": 2,
            "WR": 2,
            "TE": 1,
            "FLEX": 2,
            "OP": 1,
            "K": 1,
            "D/ST": 1,
            "BENCH": 7,
        }
    
    def _map_position(self, position_id: int) -> str:
        """Map ESPN position ID to position string"""
        position_map = {
            1: "QB",
            2: "RB",
            3: "WR",
            4: "TE",
            5: "K",
            16: "D/ST",
        }
        return position_map.get(position_id, "UNKNOWN")
    
    def _get_team_abbrev(self, team_id: int) -> str:
        """Map ESPN team ID to team abbreviation"""
        # ESPN team ID mapping (simplified)
        teams = {
            1: "ATL", 2: "BUF", 3: "CHI", 4: "CIN", 5: "CLE",
            6: "DAL", 7: "DEN", 8: "DET", 9: "GB", 10: "TEN",
            11: "IND", 12: "KC", 13: "LV", 14: "LAR", 15: "MIA",
            16: "MIN", 17: "NE", 18: "NO", 19: "NYG", 20: "NYJ",
            21: "PHI", 22: "ARI", 23: "PIT", 24: "LAC", 25: "SF",
            26: "SEA", 27: "TB", 28: "WAS", 29: "CAR", 30: "JAX",
            33: "BAL", 34: "HOU"
        }
        return teams.get(team_id, "UNK")
    
    def _extract_projected_points(self, player_data: Dict) -> float:
        """Extract projected fantasy points from player data"""
        try:
            stats = player_data.get("player", {}).get("stats", [])
            for stat in stats:
                if stat.get("statSourceId") == 1:  # Projected stats
                    return stat.get("appliedTotal", 0.0)
            return 0.0
        except:
            return 0.0
    
    def _extract_actual_points(self, player_data: Dict) -> float:
        """Extract actual fantasy points from player data"""
        try:
            stats = player_data.get("player", {}).get("stats", [])
            for stat in stats:
                if stat.get("statSourceId") == 0:  # Actual stats
                    return stat.get("appliedTotal", 0.0)
            return 0.0
        except:
            return 0.0
    
    def test_connection(self) -> bool:
        """
        Test connection to ESPN API
        
        Returns:
            True if connection successful
        """
        try:
            settings = self.get_league_settings()
            if settings:
                logger.info(f"✓ Connected to league: {settings.name}")
                logger.info(f"✓ Season: {settings.season}, Week: {settings.current_week}")
                logger.info(f"✓ Teams: {settings.num_teams}, Superflex: {settings.is_superflex}")
                return True
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False