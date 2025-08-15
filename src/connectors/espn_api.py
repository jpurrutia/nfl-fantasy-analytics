"""
ESPN Fantasy Football API Connector

Extracted and refactored from the existing draft wizard for reusability
across the NFL Analytics platform. Enhanced with league configuration detection.
"""

import requests
import json
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


@dataclass
class LeagueSettings:
    """ESPN League Settings with enhanced detection"""
    name: str
    season: int
    current_week: int
    num_teams: int
    roster_slots: Dict[str, int]
    flex_positions: Dict[str, Set[str]]  # Flex position types and eligible positions
    scoring_type: str
    scoring_details: Dict[str, float]  # Detailed scoring settings
    
    @property
    def is_superflex(self) -> bool:
        """Check if league has superflex (OP) position."""
        return "OP" in self.roster_slots
    
    @property
    def has_qb_flex(self) -> bool:
        """Check if QBs can be played in flex positions."""
        return any("QB" in eligible for eligible in self.flex_positions.values())
    
    @property
    def has_kickers(self) -> bool:
        """Check if league uses kickers."""
        return self.roster_slots.get("K", 0) > 0
    
    @property
    def has_defense(self) -> bool:
        """Check if league uses defense."""
        return self.roster_slots.get("D/ST", 0) > 0 or self.roster_slots.get("DST", 0) > 0
    
    @property
    def total_qb_slots(self) -> int:
        """Calculate total QB slots including flex."""
        base_qb = self.roster_slots.get("QB", 0)
        flex_qb = sum(
            count for pos, eligible in self.flex_positions.items()
            if "QB" in eligible
            for count in [self.roster_slots.get(pos, 0)]
        )
        return base_qb + flex_qb
    
    def to_league_config_dict(self) -> Dict[str, Any]:
        """Convert to format compatible with LeagueConfig."""
        return {
            "league_id": "espn_detected",
            "league_name": self.name,
            "league_size": self.num_teams,
            "scoring_type": self.scoring_type,
            "roster_positions": self.roster_slots,
            "flex_positions": [
                {
                    "name": pos_name,
                    "eligible_positions": list(eligible)
                }
                for pos_name, eligible in self.flex_positions.items()
            ],
            "auto_detected": True,
            "detection_source": "espn_api"
        }


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
        24: "SUPERFLEX",  # Alternative superflex designation
        25: "WR_TE",     # WR/TE flex
        26: "RB_WR",     # RB/WR flex
    }
    
    # Define flex position eligibility
    FLEX_ELIGIBILITY = {
        "FLEX": {"RB", "WR", "TE"},
        "SUPERFLEX": {"QB", "RB", "WR", "TE"},
        "OP": {"QB", "RB", "WR", "TE"},
        "WR_TE": {"WR", "TE"},
        "RB_WR": {"RB", "WR"},
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
        Get comprehensive league settings with enhanced flex detection
        
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
            
            # Parse roster slots and flex positions
            roster_slots = {}
            flex_positions = {}
            
            if "rosterSettings" in settings:
                lineup = settings["rosterSettings"].get("lineupSlotCounts", {})
                for slot_id, count in lineup.items():
                    position = self.SLOT_MAP.get(int(slot_id))
                    if position and count > 0:
                        roster_slots[position] = count
                        
                        # Check if this is a flex position
                        if position in self.FLEX_ELIGIBILITY:
                            flex_positions[position] = self.FLEX_ELIGIBILITY[position]
            else:
                roster_slots = self._get_default_roster()
                flex_positions = self._get_default_flex()
            
            # Parse detailed scoring settings
            scoring_details = {}
            scoring_type = "STANDARD"
            
            if "scoringSettings" in settings:
                scoring_items = settings["scoringSettings"].get("scoringItems", [])
                for item in scoring_items:
                    stat_id = item.get("statId")
                    points = item.get("pointsOverrides", {}).get("16", item.get("points", 0))
                    
                    # Map important scoring stats
                    if stat_id == 53:  # Receptions
                        scoring_details["receptions"] = points
                        if points == 1.0:
                            scoring_type = "PPR"
                        elif points == 0.5:
                            scoring_type = "HALF_PPR"
                    elif stat_id == 42:  # Passing yards
                        scoring_details["passing_yards"] = points
                    elif stat_id == 43:  # Passing TDs
                        scoring_details["passing_tds"] = points
                    elif stat_id == 24:  # Rushing yards
                        scoring_details["rushing_yards"] = points
                    elif stat_id == 25:  # Rushing TDs
                        scoring_details["rushing_tds"] = points
                    elif stat_id == 44:  # Receiving yards
                        scoring_details["receiving_yards"] = points
                    elif stat_id == 45:  # Receiving TDs
                        scoring_details["receiving_tds"] = points
            
            return LeagueSettings(
                name=settings.get("name", f"League {self.league_id}"),
                season=data.get("seasonId", self.year),
                current_week=data.get("scoringPeriodId", 1),
                num_teams=settings.get("size", 10),
                roster_slots=roster_slots,
                flex_positions=flex_positions,
                scoring_type=scoring_type,
                scoring_details=scoring_details
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
            flex_positions=self._get_default_flex(),
            scoring_type="PPR",
            scoring_details={"receptions": 1.0, "passing_yards": 0.04, "rushing_yards": 0.1}
        )
    
    def _get_default_roster(self) -> Dict[str, int]:
        """Return default roster configuration (your league format)"""
        return {
            "QB": 1,
            "RB": 2,
            "WR": 2,
            "TE": 1,
            "FLEX": 2,  # Traditional flex
            "OP": 1,    # QB-eligible flex
            "D/ST": 1,
            "BENCH": 7,
        }
    
    def _get_default_flex(self) -> Dict[str, Set[str]]:
        """Return default flex position eligibility"""
        return {
            "FLEX": {"RB", "WR", "TE"},
            "OP": {"QB", "RB", "WR", "TE"}
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
                logger.info(f"✓ Teams: {settings.num_teams}")
                logger.info(f"✓ Scoring: {settings.scoring_type}")
                logger.info(f"✓ QB-Flex: {settings.has_qb_flex}")
                logger.info(f"✓ Kickers: {settings.has_kickers}")
                logger.info(f"✓ Defense: {settings.has_defense}")
                logger.info(f"✓ QB Slots: {settings.total_qb_slots}")
                return True
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def detect_league_configuration(self) -> Optional[Dict[str, Any]]:
        """
        Detect complete league configuration for analytics integration.
        
        Returns:
            Dictionary compatible with LeagueConfig.from_dict()
        """
        try:
            settings = self.get_league_settings()
            if not settings:
                logger.error("Failed to detect league settings")
                return None
            
            # Convert to league config format
            config_dict = settings.to_league_config_dict()
            config_dict["league_id"] = str(self.league_id)
            
            logger.info(f"🏈 Detected league: {settings.name}")
            logger.info(f"📊 Format: {settings.scoring_type}, {settings.num_teams} teams")
            logger.info(f"🎯 Positions: {', '.join(sorted(settings.roster_slots.keys()))}")
            
            if settings.has_qb_flex:
                logger.info(f"⚡ QB-Flex league detected (OP/SUPERFLEX)")
            if not settings.has_kickers:
                logger.info(f"🚫 No kickers detected")
            
            return config_dict
            
        except Exception as e:
            logger.error(f"League configuration detection failed: {e}")
            return None