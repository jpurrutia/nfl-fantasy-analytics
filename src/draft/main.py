# enhanced_superflex_draft_tool.py

import pandas as pd
import requests
import json
import difflib
import time
import sys
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path

# Direct imports from parent modules
try:
    from ..utils.league_config import LeagueConfig, ConfigLoader, ESPNLeagueDetector
    from ..connectors.espn_api import ESPNConnector as NewESPNConnector
    LEAGUE_CONFIG_AVAILABLE = True
except ImportError:
    # Create dummy classes for type hints when imports fail
    class LeagueConfig:
        pass
    class ConfigLoader:
        pass
    class ESPNLeagueDetector:
        pass
    LEAGUE_CONFIG_AVAILABLE = False


class ESPNSuperflexConnector:
    """
    ESPN Fantasy Football connector for Superflex leagues
    """

    def __init__(self, league_id: int, year: int, swid: str, espn_s2: str):
        self.league_id = league_id
        self.year = year
        self.swid = swid if swid.startswith("{") else f"{{{swid}}}"
        self.espn_s2 = espn_s2

        self.base_url = f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{year}/segments/0/leagues/{league_id}"

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "Cookie": f"SWID={self.swid}; espn_s2={self.espn_s2}",
        }

        # Slot mapping
        self.slot_map = {
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

        print(f"✓ Connected to League {league_id} ({year} season)")
        self._verify_league()

    def _verify_league(self):
        """
        Verify connection and check for Superflex
        """
        settings = self.get_league_settings()
        if settings:
            print(f"✓ League: {settings['name']}")
            if settings["is_superflex"]:
                print("✓ SUPERFLEX LEAGUE CONFIRMED! 🏈")
            print(
                f"✓ {settings['num_teams']} teams, {settings['scoring_type']} scoring"
            )

    def get_league_settings(self) -> Dict:
        """
        Get complete league settings including roster configuration
        """
        url = f"{self.base_url}?view=mSettings"
        response = requests.get(url, headers=self.headers)

        if response.status_code != 200:
            print(f"Using default settings for {self.year}")
            # Return default settings for 2025 if not available
            return {
                "name": "Weenieless Wanderers",
                "season": self.year,
                "current_week": 0,
                "num_teams": 10,
                "roster_slots": {
                    "QB": 1,
                    "RB": 2,
                    "WR": 2,
                    "TE": 1,
                    "FLEX": 2,
                    "OP": 1,
                    "K": 1,
                    "D/ST": 1,
                    "BENCH": 7,
                },
                "is_superflex": True,
                "scoring_type": "STANDARD",
            }

        data = response.json()
        settings = data.get("settings", {})

        # Parse roster slots
        roster_slots = {}
        if "rosterSettings" in settings:
            lineup = settings["rosterSettings"].get("lineupSlotCounts", {})
            for slot_id, count in lineup.items():
                position = self.slot_map.get(int(slot_id))
                if position and count > 0:
                    roster_slots[position] = count
        else:
            # Default Superflex roster
            roster_slots = {
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

        return {
            "name": settings.get("name", "Weenieless Wanderers"),
            "season": data.get("seasonId", self.year),
            "current_week": data.get("scoringPeriodId", 0),
            "num_teams": settings.get("size", 10),
            "roster_slots": roster_slots,
            "is_superflex": roster_slots.get("OP", 0) > 0,
            "scoring_type": scoring_type,
        }


class RosterSlotManager:
    """
    Manages roster slot assignments based on league settings
    """

    def __init__(self, league_settings):
        """
        Initialize roster slots from league settings
        """
        self.roster_config = league_settings["roster_slots"]
        self.scoring_type = league_settings["scoring_type"]
        self.is_superflex = league_settings["is_superflex"]

        # Initialize empty roster slots
        self.roster = self._initialize_roster()

        # Track which positions can fill which slots
        self.slot_eligibility = {
            "QB": ["QB", "OP"],
            "RB": ["RB", "FLEX", "OP"],
            "WR": ["WR", "FLEX", "OP"],
            "TE": ["TE", "FLEX", "OP"],
            "K": ["K"],
            "D/ST": ["D/ST"],
            "D": ["D/ST"],  # Handle both formats
            "DST": ["D/ST"],  # Handle ESPN format
        }

    def _initialize_roster(self):
        """
        Create empty roster structure
        """
        roster = {}

        # Starting positions (in priority order)
        for slot_type in ["QB", "RB", "WR", "TE", "FLEX", "OP", "K", "D/ST"]:
            if slot_type in self.roster_config:
                count = self.roster_config[slot_type]
                for i in range(count):
                    slot_name = f"{slot_type}{i+1}" if count > 1 else slot_type
                    roster[slot_name] = None

        # Bench slots
        if "BENCH" in self.roster_config:
            for i in range(self.roster_config["BENCH"]):
                roster[f"BENCH{i+1}"] = None

        return roster

    def add_player(self, player_name, position, pick_number):
        """
        Add player to the best available roster slot
        """
        # Find eligible slots for this position
        eligible_slots = self.slot_eligibility.get(position, [])

        # Try to fill starting slots first
        for slot_type in eligible_slots:
            # Check numbered slots (RB1, RB2, etc.)
            for slot_name, current_player in self.roster.items():
                if slot_name.startswith(slot_type) and current_player is None:
                    self.roster[slot_name] = {
                        "name": player_name,
                        "position": position,
                        "pick": pick_number,
                        "slot": slot_name,
                    }
                    return slot_name

        # If no starting slots available, use bench
        for slot_name, current_player in self.roster.items():
            if slot_name.startswith("BENCH") and current_player is None:
                self.roster[slot_name] = {
                    "name": player_name,
                    "position": position,
                    "pick": pick_number,
                    "slot": slot_name,
                }
                return slot_name

        # No slots available (shouldn't happen in normal draft)
        return None

    def get_needs_analysis(self):
        """
        Analyze what positions are still needed
        """
        needs = {
            "critical": [],  # Starting positions unfilled
            "important": [],  # Flex/OP positions unfilled
            "depth": [],  # Bench considerations
        }

        for slot_name, player in self.roster.items():
            if player is None:
                if slot_name.startswith("BENCH"):
                    needs["depth"].append(slot_name)
                elif slot_name.startswith(("FLEX", "OP")):
                    needs["important"].append(slot_name)
                else:
                    needs["critical"].append(slot_name)

        return needs

    def get_position_summary(self):
        """
        Get count of each position on roster
        """
        summary = {
            "QB": {"starters": 0, "bench": 0, "total": 0},
            "RB": {"starters": 0, "bench": 0, "total": 0},
            "WR": {"starters": 0, "bench": 0, "total": 0},
            "TE": {"starters": 0, "bench": 0, "total": 0},
            "K": {"starters": 0, "bench": 0, "total": 0},
            "D/ST": {"starters": 0, "bench": 0, "total": 0},
        }

        for slot_name, player in self.roster.items():
            if player:
                pos = player["position"]
                # Handle all defense formats -> 'D/ST' conversion
                if pos in ["D", "DST"]:
                    pos = "D/ST"

                if pos in summary:
                    if slot_name.startswith("BENCH"):
                        summary[pos]["bench"] += 1
                    else:
                        summary[pos]["starters"] += 1
                    summary[pos]["total"] += 1

        return summary


class LeagueAwareDraftManager:
    """
    ENHANCED: Draft manager with automatic league detection and adaptation
    """

    def __init__(self, league_id: int, year: int, swid: str, espn_s2: str, adp_csv_path: str):
        self.league_id = league_id
        self.year = year
        
        # Initialize league configuration
        if LEAGUE_CONFIG_AVAILABLE:
            self.league_config = self._detect_league_config(league_id, swid, espn_s2)
            self.use_legacy_mode = False
            print("🚀 Using LEAGUE-AWARE MODE with auto-detection")
        else:
            self.league_config = None
            self.use_legacy_mode = True
            print("⚠️ Using legacy mode - league configuration not available")
        
        # Initialize ESPN connector (fallback to legacy if needed)
        if self.use_legacy_mode:
            self.espn = ESPNSuperflexConnector(league_id, year, swid, espn_s2)
            self.settings = self.espn.get_league_settings()
        else:
            # Use league config settings
            self.settings = self._convert_league_config_to_settings()
            
        # Initialize roster slot manager
        self.roster_manager = RosterSlotManager(self.settings)

        # Load and adjust ADP data with league-aware enhancements
        self.players = self.load_and_adjust_adp(adp_csv_path)

        # Track draft state
        self.drafted_players = []
        self.my_team = []
        self.current_pick = 1

        # State persistence setup
        league_name_clean = self.settings["name"].replace(" ", "_").replace("/", "_")
        self.state_file = f"draft_state_{league_name_clean}.json"
        self.backup_dir = Path("draft_backups")
        self.backup_dir.mkdir(exist_ok=True)

        print(f"✅ Loaded {len(self.players)} players")
        if not self.use_legacy_mode:
            print(f"🏈 League format: {self.league_config.scoring_type}")
            print(f"📊 Positions: {', '.join(sorted(self.league_config.all_eligible_positions))}")
            if self.league_config.has_qb_flex:
                print(f"⚡ QB-Flex league: {self.league_config.total_qb_slots} QB slots")
            if not self.league_config.has_kickers:
                print(f"🚫 No kickers in this league")
        print(f"🎯 Roster slots: {self._get_roster_slots_summary()}")

        # Auto-load if state file exists
        self.load_draft_state()

    def _detect_league_config(self, league_id: int, swid: str, espn_s2: str) -> Optional[LeagueConfig]:
        """Detect league configuration using the new system."""
        try:
            detector = ESPNLeagueDetector()
            return detector.detect_league_config(str(league_id), swid, espn_s2)
        except Exception as e:
            print(f"⚠️ League detection failed: {e}")
            return None

    def _convert_league_config_to_settings(self) -> Dict:
        """Convert LeagueConfig to legacy settings format."""
        if not self.league_config:
            # Fallback to legacy detection
            espn = ESPNSuperflexConnector(self.league_id, self.year, "", "")
            return espn.get_league_settings()
        
        return {
            "name": self.league_config.league_name,
            "season": self.year,
            "current_week": 0,
            "num_teams": self.league_config.league_size,
            "roster_slots": self.league_config.roster_positions,
            "is_superflex": self.league_config.has_qb_flex,
            "scoring_type": self.league_config.scoring_type,
        }

    def get_eligible_positions(self) -> List[str]:
        """Get positions that exist in this league."""
        if self.league_config:
            return sorted(list(self.league_config.all_eligible_positions))
        else:
            # Legacy fallback
            return ["QB", "RB", "WR", "TE", "K", "D/ST"]

    def position_exists_in_league(self, position: str) -> bool:
        """Check if position exists in current league."""
        eligible = self.get_eligible_positions()
        return position in eligible

    def get_qb_value_multiplier(self) -> float:
        """Get QB value multiplier for this league format."""
        if self.league_config:
            return self.league_config.qb_value_multiplier
        else:
            # Legacy detection
            return 1.5 if self.settings.get("is_superflex", False) else 1.0

    def should_defer_position(self, position: str) -> bool:
        """Determine if position should be deferred to late rounds."""
        if not self.league_config:
            # Legacy behavior
            return position in ["K", "D/ST"] and not self.is_late_round()
        
        # New logic: defer if position doesn't exist in league OR if K/DST in early rounds
        if position not in self.league_config.all_eligible_positions:
            return True
            
        if position in ["K", "D/ST"] and not self.is_late_round():
            return True
            
        return False

    def _get_roster_slots_summary(self):
        """Get summary of roster configuration."""
        config = self.settings["roster_slots"]
        slots = []

        # Starting positions - only show positions that exist in league
        eligible_positions = self.get_eligible_positions()
        
        for pos in ["QB", "RB", "WR", "TE", "FLEX", "OP", "K", "D/ST"]:
            if pos in config and config[pos] > 0:
                # Check if position exists in league (for display purposes)
                if pos in ["FLEX", "OP"] or any(eligible_pos.startswith(pos) for eligible_pos in eligible_positions):
                    slots.append(f"{config[pos]}{pos}")

        # Bench
        if "BENCH" in config:
            slots.append(f"{config['BENCH']}BN")

        return ", ".join(slots)

    def get_current_round(self) -> int:
        """Calculate the current round based on pick number and league size"""
        num_teams = self.settings.get("num_teams", 10)
        return ((self.current_pick - 1) // num_teams) + 1

    def get_total_rounds(self) -> int:
        """Calculate total number of rounds in the draft"""
        total_roster_spots = sum(self.roster_manager.roster_config.values())
        return total_roster_spots

    def is_late_round(self) -> bool:
        """Determine if we're in the last 2 rounds of the draft"""
        current_round = self.get_current_round()
        total_rounds = self.get_total_rounds()
        return current_round >= (total_rounds - 1)  # Last 2 rounds

    def load_and_adjust_adp(self, csv_path: str) -> pd.DataFrame:
        """Load ADP data and adjust for league format with ENHANCED league awareness"""
        # Load your ADP CSV
        df = pd.read_csv(csv_path)

        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        # Extract position from POS column (e.g., "WR1" -> "WR")
        df["position"] = df["pos"].str.extract(r"([A-Z]+)")

        # Standardize defense positions
        df.loc[df["position"].isin(["D", "DST"]), "position"] = "D/ST"

        # ENHANCED: Filter out positions that don't exist in this league
        eligible_positions = self.get_eligible_positions()
        original_count = len(df)
        df = df[df["position"].isin(eligible_positions)]
        filtered_count = original_count - len(df)
        
        if filtered_count > 0:
            print(f"📊 Filtered out {filtered_count} players from positions not in this league")

        # Use ESPN column for primary ADP
        df["adp_standard"] = df["espn"].fillna(df["avg"])

        # ENHANCED: League-aware ADP adjustment
        def adjust_league_aware_adp(row):
            if row["position"] not in eligible_positions:
                return 999  # Push filtered players to bottom
            
            qb_multiplier = self.get_qb_value_multiplier()
            
            if row["position"] != "QB":
                # Non-QBs: slight adjustment based on league format
                return row["adp_standard"] + (5 if qb_multiplier > 1.0 else 0)

            orig_adp = row["adp_standard"]
            
            # QB adjustments based on league format
            if qb_multiplier > 1.0:  # QB-flex league
                # Scale QB ADPs based on tier
                if orig_adp <= 50:
                    new_adp = orig_adp * 0.3  # Elite QBs move up significantly
                elif orig_adp <= 80:
                    new_adp = orig_adp * 0.5  # Mid QBs move up moderately
                else:
                    new_adp = orig_adp * 0.7  # Late QBs move up slightly
            else:  # Standard league
                new_adp = orig_adp  # No QB adjustment

            return max(1, min(new_adp, orig_adp))

        df["adp_superflex"] = df.apply(adjust_league_aware_adp, axis=1)

        # Add league-aware projections
        df["projected_points"] = self.estimate_projections(df)

        # Add tier assignments
        df["tier"] = self.assign_tiers(df)

        # Sort by league-adjusted ADP
        df = df.sort_values("adp_superflex").reset_index(drop=True)

        return df

    def estimate_projections(self, df: pd.DataFrame) -> pd.Series:
        """More realistic projection estimates with league awareness"""
        projections = []
        qb_multiplier = self.get_qb_value_multiplier()
        
        for _, row in df.iterrows():
            pos = row["position"]
            adp = row["adp_standard"]

            if pos == "QB":
                # Adjust QB projections based on league format
                base_multiplier = qb_multiplier
                if adp <= 10:
                    proj = (400 - (adp * 3)) * base_multiplier
                elif adp <= 30:
                    proj = (370 - (adp * 2)) * base_multiplier
                elif adp <= 60:
                    proj = (320 - (adp * 1)) * base_multiplier
                else:
                    proj = (250 - (adp * 0.5)) * base_multiplier

            elif pos == "RB":
                # Steeper early, flatter later
                if adp <= 5:
                    proj = 300 - (adp * 8)
                elif adp <= 15:
                    proj = 270 - (adp * 4)
                elif adp <= 30:
                    proj = 230 - (adp * 2)
                else:
                    proj = 180 - (adp * 0.5)

            elif pos == "WR":
                # Similar to RB but slightly flatter
                if adp <= 5:
                    proj = 280 - (adp * 6)
                elif adp <= 15:
                    proj = 250 - (adp * 3)
                elif adp <= 30:
                    proj = 210 - (adp * 1.5)
                else:
                    proj = 170 - (adp * 0.5)

            elif pos == "TE":
                if adp <= 10:
                    proj = 220 - (adp * 5)
                elif adp <= 30:
                    proj = 180 - (adp * 2)
                else:
                    proj = 120
            else:
                proj = 100

            projections.append(max(50, proj))

        return pd.Series(projections)

    def assign_tiers(self, df: pd.DataFrame) -> pd.Series:
        """League-aware tier assignment"""
        tiers = []

        # First, sort by position and ADP
        for idx, row in df.iterrows():
            pos = row["position"]

            # Get all players at this position, sorted by ADP
            pos_players = (
                df[df["position"] == pos]
                .sort_values("adp_superflex")
                .reset_index(drop=True)
            )

            # Find this player's rank at position
            player_name = row["player"]
            pos_rank = (
                pos_players[pos_players["player"] == player_name].index[0] + 1
                if len(pos_players[pos_players["player"] == player_name]) > 0
                else 99
            )

            # Assign tier based on positional rank
            if pos_rank <= 4:
                tier = 1
            elif pos_rank <= 10:
                tier = 2
            elif pos_rank <= 20:
                tier = 3
            elif pos_rank <= 35:
                tier = 4
            else:
                tier = 5

            tiers.append(tier)

        return pd.Series(tiers, index=df.index)

    def get_available_players(self) -> pd.DataFrame:
        """Get all undrafted players (league-filtered)"""
        available = self.players[~self.players["player"].isin(self.drafted_players)]
        
        # Additional filtering: only show positions that exist in league
        eligible_positions = self.get_eligible_positions()
        return available[available["position"].isin(eligible_positions)]

    def calculate_value_score(self, player_row, current_pick: int) -> float:
        """Calculate how much value a player represents with league awareness"""
        # Basic value: how far past ADP
        adp_value = current_pick - player_row["adp_superflex"]

        # Tier bonus (higher tier = more valuable)
        tier_bonus = (6 - player_row["tier"]) * 2

        # Enhanced positional scarcity bonus with league awareness
        position = player_row["position"]
        available = self.get_available_players()
        pos_available = available[available["position"] == position]

        scarcity_bonus = 0
        if position == "QB" and len(pos_available) < 15:
            # Adjust QB scarcity based on league format
            qb_multiplier = self.get_qb_value_multiplier()
            scarcity_bonus = 15 if qb_multiplier > 1.0 else 10
        elif position == "RB" and len(pos_available) < 20:
            scarcity_bonus = 5
        elif position in ["K", "D/ST"] and not self.position_exists_in_league(position):
            scarcity_bonus = -20  # Penalize positions not in league

        return adp_value + tier_bonus + scarcity_bonus

    def calculate_team_need_score(self, player_row) -> float:
        """Enhanced need score with league-aware position filtering"""
        position = player_row["position"]
        needs = self.roster_manager.get_needs_analysis()
        summary = self.roster_manager.get_position_summary()

        # ENHANCED: Check if position exists in league
        if not self.position_exists_in_league(position):
            return -100  # Heavily penalize positions not in league

        # ENHANCED: Use league-aware deferral logic
        if self.should_defer_position(position):
            return -50  # Defer K/D/ST or positions not in league

        # Check critical needs first (unfilled starters)
        for slot in needs["critical"]:
            # Direct position match
            slot_base = slot.replace("1", "").replace("2", "").replace("3", "")
            if slot_base == position:
                if not self.should_defer_position(position):
                    return 20  # Highest priority
            # Special case for defenses (handle all formats)
            if position in ["D", "DST", "D/ST"] and slot.startswith("D/ST"):
                if not self.should_defer_position(position):
                    return 20

        # Check flex/OP eligibility for important needs
        if position in ["RB", "WR", "TE", "QB"]:
            for slot in needs["important"]:
                if slot.startswith("FLEX") and position in ["RB", "WR", "TE"]:
                    return 10
                if slot.startswith("OP"):  # Any offensive position
                    return 10

        # Check if we've hit position caps
        display_position = position
        if position in ["D", "DST"]:
            display_position = "D/ST"

        pos_data = summary.get(display_position, {"total": 0})

        # League-aware position targets
        position_targets = {
            "QB": 3 if self.league_config and self.league_config.has_qb_flex else 2,
            "RB": 6,
            "WR": 7,
            "TE": 2,
            "K": 1 if self.position_exists_in_league("K") else 0,
            "D": 1 if self.position_exists_in_league("D/ST") else 0,
            "DST": 1 if self.position_exists_in_league("D/ST") else 0,
            "D/ST": 1 if self.position_exists_in_league("D/ST") else 0,
        }

        target = position_targets.get(position, 0)
        if target == 0 or pos_data["total"] >= target:
            return 0  # No need

        # Calculate depth need
        remaining_need = target - pos_data["total"]
        return min(5, remaining_need * 2)  # Depth picks worth less

    def get_draft_recommendation(self, num_recommendations: int = 5) -> pd.DataFrame:
        """Get top recommended picks with league-aware smart position balancing"""
        available = self.get_available_players()

        if available.empty:
            return pd.DataFrame()

        # ENHANCED: League-aware filtering - remove positions not in league
        eligible_positions = self.get_eligible_positions()
        available = available[available["position"].isin(eligible_positions)]

        # ENHANCED: Filter out positions that should be deferred
        if not self.is_late_round():
            # Remove positions that should be deferred (K/D/ST or positions not in league)
            available = available[
                ~available["position"].apply(self.should_defer_position)
            ]

        # Calculate scores for each player
        recommendations = []
        for _, player in available.iterrows():
            value_score = self.calculate_value_score(player, self.current_pick)
            need_score = self.calculate_team_need_score(player)

            # ENHANCED: Boost need weighting when critical needs exist
            needs = self.roster_manager.get_needs_analysis()

            # Filter out deferred positions from critical needs 
            if not self.is_late_round():
                critical_skill_needs = [
                    slot
                    for slot in needs["critical"]
                    if not self.should_defer_position(slot.replace("1", "").replace("2", "").replace("3", ""))
                ]
            else:
                critical_skill_needs = needs["critical"]

            if critical_skill_needs:
                # More emphasis on need when we have unfilled starters
                total_score = (value_score * 0.4) + (need_score * 0.6)
            else:
                # Standard weighting when just looking for depth
                total_score = (value_score * 0.6) + (need_score * 0.4)

            recommendations.append(
                {
                    "player": player["player"],
                    "position": player["position"],
                    "team": player["team"],
                    "tier": player["tier"],
                    "adp": player["adp_superflex"],
                    "projected": player["projected_points"],
                    "value_score": round(value_score, 1),
                    "need_score": round(need_score, 1),
                    "total_score": round(total_score, 1),
                    "pick_value": (
                        "GREAT"
                        if value_score > 10
                        else "GOOD" if value_score > 5 else "FAIR"
                    ),
                }
            )

        df_rec = pd.DataFrame(recommendations)
        df_rec = df_rec.sort_values("total_score", ascending=False)

        # ENHANCED: League-aware position balancing for recommendations
        return self._balance_position_recommendations(df_rec, num_recommendations)

    def _balance_position_recommendations(
        self, df_rec: pd.DataFrame, num_recommendations: int
    ) -> pd.DataFrame:
        """Balance recommendations to avoid over-showing saturated positions with league awareness"""
        if df_rec.empty:
            return df_rec

        summary = self.roster_manager.get_position_summary()
        needs = self.roster_manager.get_needs_analysis()

        # League-aware position targets
        position_targets = {
            "QB": 3 if self.league_config and self.league_config.has_qb_flex else 2,
            "RB": 6,
            "WR": 7,
            "TE": 2,
            "K": 1 if self.position_exists_in_league("K") else 0,
            "D/ST": 1 if self.position_exists_in_league("D/ST") else 0,
        }

        saturated_positions = set()
        for pos, target in position_targets.items():
            if target == 0:  # Position doesn't exist in league
                saturated_positions.add(pos)
                continue
            pos_data = summary.get(pos, {"total": 0})
            if pos_data["total"] >= target:
                saturated_positions.add(pos)

        # If we have critical needs, limit saturated positions more aggressively
        max_saturated_recs = 1 if needs["critical"] else 2

        balanced_recs = []
        saturated_count = {}

        for _, rec in df_rec.iterrows():
            pos = rec["position"]
            if pos in ["D", "DST"]:
                pos = "D/ST"

            # If position is saturated or doesn't exist in league, limit how many we show
            if pos in saturated_positions:
                current_count = saturated_count.get(pos, 0)
                if current_count >= max_saturated_recs:
                    continue  # Skip this recommendation
                saturated_count[pos] = current_count + 1

            balanced_recs.append(rec)

            # Stop when we have enough recommendations
            if len(balanced_recs) >= num_recommendations:
                break

        return pd.DataFrame(balanced_recs)

    def draft_player(self, player_name: str, team: str = "my_team"):
        """Mark a player as drafted with enhanced league-aware validation"""
        # Clean up the input
        player_name = player_name.strip()

        # Filter out rows with NaN player names
        valid_players = self.players[self.players["player"].notna()].copy()
        
        # ENHANCED: Additional filtering for league eligibility
        eligible_positions = self.get_eligible_positions()
        valid_players = valid_players[valid_players["position"].isin(eligible_positions)]

        # Try exact match first (case insensitive)
        exact_matches = valid_players[
            valid_players["player"].str.lower() == player_name.lower()
        ]

        if not exact_matches.empty:
            player_matches = exact_matches
        else:
            # Enhanced fuzzy matching with league awareness
            player_matches = self._smart_fuzzy_match(player_name, valid_players)
            if player_matches is None:
                return False

        actual_player_name = player_matches.iloc[0]["player"]
        position = player_matches.iloc[0]["position"]

        # ENHANCED: League-aware position validation
        if not self.position_exists_in_league(position):
            print(f"❌ {position} position is not available in this league!")
            print(f"💡 Available positions: {', '.join(self.get_eligible_positions())}")
            return False

        # Check if already drafted
        if actual_player_name in self.drafted_players:
            print(f"⚠️ {actual_player_name} has already been drafted!")
            return False

        self.drafted_players.append(actual_player_name)

        if team == "my_team":
            player_data = player_matches.iloc[0]

            # Fix defense position handling
            if position in ["D", "DST"]:
                position = "D/ST"

            # Add to roster manager
            slot_filled = self.roster_manager.add_player(
                actual_player_name, position, self.current_pick
            )

            # Check if roster is full
            if slot_filled is None:
                print(f"❌ No roster slots available for {actual_player_name}")
                print("⚠️ Your roster is full - this pick cannot be added!")
                # Undo the drafted player addition
                self.drafted_players.remove(actual_player_name)
                return False

            self.my_team.append(
                {
                    "player": actual_player_name,
                    "position": position,
                    "pick": self.current_pick,
                    "slot": slot_filled,
                }
            )
            
            # Enhanced success message with league context
            league_context = ""
            if self.league_config:
                if position == "QB" and self.league_config.has_qb_flex:
                    league_context = " [QB-FLEX LEAGUE]"
                elif position in ["K", "D/ST"] and not self.position_exists_in_league(position):
                    league_context = " [POSITION NOT IN LEAGUE!]"
            
            print(
                f"✓ Drafted {actual_player_name} ({position}) to {slot_filled} at pick {self.current_pick}{league_context}"
            )
        else:
            if position in ["D", "DST"]:
                position = "D/ST"
            print(
                f"✓ {actual_player_name} ({position}) drafted by another team at pick {self.current_pick}"
            )

        self.current_pick += 1

        # Auto-save after each successful pick
        self.save_draft_state()

        return True

    def _smart_fuzzy_match(self, player_name: str, valid_players: pd.DataFrame):
        """Enhanced fuzzy matching with league-aware position filtering"""
        search_lower = player_name.lower()

        # Check if this looks like a defense search
        is_defense_search = self._is_defense_search(search_lower, valid_players)

        if is_defense_search:
            # Only search among defenses that exist in this league
            if self.position_exists_in_league("D/ST"):
                defense_players = valid_players[
                    valid_players["position"].isin(["D", "DST", "D/ST"])
                ]
                if not defense_players.empty:
                    defense_names = defense_players["player"].str.lower().tolist()
                    close_matches = difflib.get_close_matches(
                        search_lower,
                        defense_names,
                        n=3,
                        cutoff=0.3,  # Lower cutoff for defenses
                    )

                    if close_matches:
                        if len(close_matches) == 1:
                            matched_name = close_matches[0]
                            player_matches = defense_players[
                                defense_players["player"].str.lower() == matched_name
                            ]
                            print(f"📝 Auto-matched to: {player_matches.iloc[0]['player']}")
                            return player_matches
                        else:
                            print(f"Multiple defense matches for '{player_name}':")
                            for match in close_matches:
                                player_info = defense_players[
                                    defense_players["player"].str.lower() == match
                                ].iloc[0]
                                print(
                                    f"  - {player_info['player']} ({player_info['position']})"
                                )
                            return None
            else:
                print(f"❌ Defense/DST positions are not available in this league!")
                print(f"💡 Available positions: {', '.join(self.get_eligible_positions())}")
                return None

        # Regular fuzzy matching for non-defense players
        all_names = valid_players["player"].str.lower().tolist()
        close_matches = difflib.get_close_matches(
            search_lower, all_names, n=5, cutoff=0.6
        )

        if not close_matches:
            print(f"❌ No matches found for '{player_name}'")
            # Show suggestions based on first few letters
            suggestions = valid_players[
                valid_players["player"]
                .str.lower()
                .str.startswith(
                    search_lower[:3] if len(search_lower) >= 3 else search_lower
                )
            ].head(3)
            if not suggestions.empty:
                print("Did you mean:")
                for _, p in suggestions.iterrows():
                    print(f"  - {p['player']} ({p['position']}, {p['team']})")
            return None

        if len(close_matches) == 1:
            # Auto-select if only one close match
            matched_name = close_matches[0]
            player_matches = valid_players[
                valid_players["player"].str.lower() == matched_name
            ]

            # Safety check - don't auto-match if positions are completely different
            matched_player = player_matches.iloc[0]
            if self._is_reasonable_match(player_name, matched_player):
                print(f"📝 Auto-matched to: {matched_player['player']}")
                return player_matches
            else:
                print(
                    f"❌ '{player_name}' doesn't seem to match '{matched_player['player']}' ({matched_player['position']})"
                )
                return None
        else:
            # Show options
            print(f"Multiple matches for '{player_name}':")
            for match in close_matches:
                player_info = valid_players[
                    valid_players["player"].str.lower() == match
                ].iloc[0]
                print(
                    f"  - {player_info['player']} ({player_info['position']}, {player_info['team']})"
                )
            return None

    def _is_defense_search(self, search_term: str, valid_players: pd.DataFrame) -> bool:
        """Dynamically determine if search term is looking for a defense with league awareness"""
        # First check if defenses even exist in this league
        if not self.position_exists_in_league("D/ST"):
            return False
            
        # Obvious defense keywords
        defense_keywords = ["defense", "dst", "d/st", "def"]
        if any(keyword in search_term for keyword in defense_keywords):
            return True

        # Get all defense team names from the actual data
        defense_players = valid_players[
            valid_players["position"].isin(["D", "DST", "D/ST"])
        ]
        if defense_players.empty:
            return False

        # Extract team names and cities from defense player names
        defense_terms = set()
        for defense_name in defense_players["player"].str.lower():
            # Split defense names and extract potential team identifiers
            words = (
                defense_name.replace("defense", "")
                .replace("dst", "")
                .replace("d/st", "")
                .split()
            )
            for word in words:
                if len(word) > 2:  # Skip short words like "d", "st"
                    defense_terms.add(word)

        # Check if search term matches any actual defense team identifier
        search_words = search_term.split()
        for word in search_words:
            if word in defense_terms:
                return True

        # Check for partial matches with defense terms
        for defense_term in defense_terms:
            if len(search_term) >= 4 and (
                search_term in defense_term or defense_term in search_term
            ):
                return True

        return False

    def _is_reasonable_match(self, search_term: str, matched_player: pd.Series) -> bool:
        """Check if a fuzzy match makes sense with league awareness"""
        search_lower = search_term.lower()
        player_name_lower = matched_player["player"].lower()
        position = matched_player["position"]

        # Check if position exists in league
        if not self.position_exists_in_league(position):
            return False

        # If already determined to be a defense search, only allow defense matches
        if self._is_defense_search(search_lower, pd.DataFrame([matched_player])):
            return position in ["D", "DST", "D/ST"]

        # If search term and match share at least 3 characters, probably OK
        common_chars = set(search_lower) & set(player_name_lower)
        if len(common_chars) >= 3:
            return True

        # Otherwise be conservative
        return False

    # Inherit remaining methods from SuperflexDraftManager with some adaptations
    def drop_player(self, player_name: str):
        """Drop a player from your roster to make room (inherited from SuperflexDraftManager)"""
        return SuperflexDraftManager.drop_player(self, player_name)

    def undo_last_pick(self):
        """Undo your most recent draft pick (inherited from SuperflexDraftManager)"""
        return SuperflexDraftManager.undo_last_pick(self)

    def save_draft_state(self, manual_save=False):
        """Save current draft state with league configuration"""
        state = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "league_info": {
                "name": self.settings["name"],
                "league_id": self.league_id,
                "year": self.year,
                "league_config_available": not self.use_legacy_mode,
                "scoring_type": self.league_config.scoring_type if self.league_config else self.settings.get("scoring_type", "Unknown"),
                "has_qb_flex": self.league_config.has_qb_flex if self.league_config else self.settings.get("is_superflex", False),
                "eligible_positions": self.get_eligible_positions()
            },
            "draft_progress": {
                "current_pick": self.current_pick,
                "drafted_players": self.drafted_players.copy(),
                "my_team": self.my_team.copy(),
            },
            "roster_state": {
                "roster": {k: v for k, v in self.roster_manager.roster.items()},
                "roster_config": self.roster_manager.roster_config,
            },
        }

        try:
            # Save main state file
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)

            # Create timestamped backup
            backup_file = (
                self.backup_dir / f"draft_backup_{time.strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(backup_file, "w") as f:
                json.dump(state, f, indent=2)

            # Clean old backups (keep last 10)
            self._cleanup_old_backups()

            if manual_save:
                print(f"✓ Manual save completed (Pick {self.current_pick})")
                if self.league_config:
                    print(f"💾 League configuration saved: {self.league_config.league_name}")

        except Exception as e:
            print(f"⚠️ Save failed: {e}")

    def load_draft_state(self):
        """Load draft state from file if it exists (inherited with enhancements)"""
        return SuperflexDraftManager.load_draft_state(self)

    def _cleanup_old_backups(self):
        """Keep only the 10 most recent backup files (inherited)"""
        return SuperflexDraftManager._cleanup_old_backups(self)

    def show_draft_status(self):
        """Show comprehensive draft status with league awareness"""
        print("\n📊 DRAFT STATUS SUMMARY")
        print("=" * 40)
        print(f"Current Pick: {self.current_pick}")
        print(f"Round: {self.get_current_round()}/{self.get_total_rounds()}")
        print(f"Players Drafted: {len(self.drafted_players)}")
        print(f"My Team Size: {len(self.my_team)}")
        
        # League configuration status
        if self.league_config:
            print(f"\n🏈 League: {self.league_config.league_name} ({self.league_config.scoring_type})")
            print(f"📊 Positions: {', '.join(sorted(self.league_config.all_eligible_positions))}")
            if self.league_config.has_qb_flex:
                print(f"⚡ QB-Flex league: {self.league_config.total_qb_slots} QB slots")
            if not self.league_config.has_kickers:
                print(f"🚫 No kickers in this league")
        else:
            print(f"\n⚠️ Using legacy mode - limited league detection")

        if self.my_team:
            print(f"\nMy Picks:")
            for player in self.my_team:
                print(
                    f"  Pick {player['pick']}: {player['player']} ({player['position']}) → {player.get('slot', 'N/A')}"
                )

        # Show most recent drafts
        if len(self.drafted_players) > 0:
            recent_picks = self.drafted_players[-5:]  # Last 5 picks
            print(f"\nRecent Picks: {', '.join(recent_picks)}")

    def load_from_backup(self):
        """Interactive backup file selection (inherited)"""
        return SuperflexDraftManager.load_from_backup(self)

    def show_draft_board(self):
        """Display current draft state with enhanced league awareness"""
        print("\n" + "=" * 60)
        print(
            f"PICK {self.current_pick} - ROUND {self.get_current_round()}/{self.get_total_rounds()} - LEAGUE-AWARE RECOMMENDATIONS"
        )
        print("=" * 60)
        
        # Show league configuration info
        if self.league_config:
            print(f"🏈 {self.league_config.league_name} ({self.league_config.scoring_type})")
            print(f"📊 Positions: {', '.join(sorted(self.league_config.all_eligible_positions))}")
            if self.league_config.has_qb_flex:
                print(f"⚡ QB-Flex league: Enhanced QB values")
            if not self.league_config.has_kickers:
                print(f"🚫 No kickers - Focus on skill positions")
        else:
            print(f"⚠️ Legacy mode - Limited league awareness")

        # Show position deferral strategy message
        deferred_positions = [pos for pos in self.get_eligible_positions() if self.should_defer_position(pos)]
        if deferred_positions and not self.is_late_round():
            rounds_until_late = self.get_total_rounds() - self.get_current_round()
            print(
                f"\n📌 Strategy: Deferring {', '.join(deferred_positions)} for {rounds_until_late} more rounds"
            )

        # Show roster with slots filled
        self._display_roster_slots()

        # Show roster capacity
        self.show_roster_capacity()

    def show_roster_capacity(self):
        """Show current roster capacity info with league awareness"""
        total_slots = sum(self.roster_manager.roster_config.values())
        filled_slots = len(
            [p for p in self.roster_manager.roster.values() if p is not None]
        )

        print(f"\n📊 Roster Capacity: {filled_slots}/{total_slots} slots filled")

        if filled_slots >= total_slots:
            print("⚠️ ROSTER FULL - No more picks can be added!")
        else:
            remaining = total_slots - filled_slots
            print(f"✅ {remaining} slots remaining")

        # Show needs analysis with league-aware filtering
        needs = self.roster_manager.get_needs_analysis()

        # Filter positions based on league eligibility and deferral logic
        if not self.is_late_round():
            critical_filtered = [
                slot for slot in needs["critical"] 
                if not self.should_defer_position(slot.replace("1", "").replace("2", "").replace("3", ""))
            ]
            deferred_needs = [
                slot for slot in needs["critical"] 
                if self.should_defer_position(slot.replace("1", "").replace("2", "").replace("3", ""))
            ]
        else:
            critical_filtered = needs["critical"]
            deferred_needs = []

        if critical_filtered:
            print("\n⚠️  Critical Needs (Starting Positions):")
            print(f"   {', '.join(critical_filtered)}")
        if deferred_needs:
            print("\n⏳ Deferred Needs (Will address in final rounds):")
            print(f"   {', '.join(deferred_needs)}")
        if needs["important"]:
            print("\n📊 Important Needs (Flex/OP):")
            print(f"   {', '.join(needs['important'])}")

        # Show recommendations
        recs = self.get_draft_recommendation()

        if not recs.empty:
            print("\n🎯 Top Recommendations (League-Aware Best Value):")
            for _, rec in recs.iterrows():
                # Add indicator if fills critical need
                need_indicator = ""
                for slot in critical_filtered:  # Use filtered list
                    slot_base = slot.replace("1", "").replace("2", "").replace("3", "")
                    if rec["position"] == slot_base:
                        need_indicator = " ⭐ [FILLS STARTER NEED]"
                        break
                
                # Add league context indicators
                league_indicator = ""
                if self.league_config:
                    if rec["position"] == "QB" and self.league_config.has_qb_flex:
                        league_indicator = " [QB-FLEX VALUE]"
                    elif rec["position"] in ["K", "D/ST"] and not self.position_exists_in_league(rec["position"]):
                        league_indicator = " [NOT IN LEAGUE]"

                print(
                    f"\n{rec['player']} ({rec['position']}) - {rec['team']}{need_indicator}{league_indicator}"
                )
                print(
                    f"  Tier: {rec['tier']} | ADP: {rec['adp']:.1f} | Proj: {rec['projected']:.0f}"
                )
                print(
                    f"  Value: {rec['value_score']} | Need: {rec['need_score']} | TOTAL: {rec['total_score']}"
                )
                print(f"  Verdict: {rec['pick_value']} VALUE")

        # Show best available at each needed position
        self._display_positional_needs()

        # Show position summary
        self._display_position_summary()

    def _display_positional_needs(self):
        """Display best available players at each needed position with league awareness"""
        needs = self.roster_manager.get_needs_analysis()
        available = self.get_available_players()

        if not needs["critical"] and not needs["important"]:
            return

        print("\n🔍 Best Available at Positional Needs (League-Aware):")
        print("-" * 50)

        # Get positions we actually need
        needed_positions = set()

        # Critical needs (starting positions)
        for slot in needs["critical"]:
            pos = slot.replace("1", "").replace("2", "").replace("3", "")
            # Only include positions that exist in league and aren't deferred
            if self.position_exists_in_league(pos) and not self.should_defer_position(pos):
                needed_positions.add(pos)

        # Important needs (FLEX/OP eligible positions)
        if needs["important"]:
            # Only add positions that exist in this league
            for pos in ["QB", "RB", "WR", "TE"]:
                if self.position_exists_in_league(pos):
                    needed_positions.add(pos)

        # Show top 2-3 at each needed position
        eligible_positions = self.get_eligible_positions()
        for position in ["QB", "RB", "WR", "TE", "K", "D/ST"]:
            if position not in needed_positions or position not in eligible_positions:
                continue

            # Handle defense format variations
            if position == "D/ST":
                pos_players = available[
                    available["position"].isin(["D/ST", "D", "DST"])
                ].copy()
            else:
                pos_players = available[available["position"] == position].copy()

            if pos_players.empty:
                continue

            # Sort by ADP (best available first)
            pos_players = pos_players.sort_values("adp_superflex").head(3)

            # Determine if this is critical or important need
            is_critical = any(
                slot.replace("1", "").replace("2", "").replace("3", "") == position
                or (position == "D/ST" and slot.startswith("D/ST"))
                for slot in needs["critical"]
            )

            # Special handling for deferred positions
            if self.should_defer_position(position):
                need_type = "⏳ DEFERRED"
            elif is_critical:
                need_type = "🚨 CRITICAL"
            else:
                need_type = "📋 FLEX/OP"

            print(f"\n{position} ({need_type}):")

            for i, (_, player) in enumerate(pos_players.iterrows()):
                # Calculate value for this pick
                value_score = self.calculate_value_score(player, self.current_pick)

                # Add league context
                league_note = ""
                if position == "QB" and self.league_config and self.league_config.has_qb_flex:
                    league_note = " [QB-FLEX]"

                # Simple display
                print(
                    f"  {i+1}. {player['player']} - {player['team']} "
                    f"(ADP: {player['adp_superflex']:.1f}, Value: {value_score:+.1f}){league_note}"
                )

    def _display_roster_slots(self):
        """Display current roster with slot assignments and league awareness"""
        return SuperflexDraftManager._display_roster_slots(self)

    def _display_position_summary(self):
        """Display position summary with league-aware starter/bench breakdown"""
        summary = self.roster_manager.get_position_summary()

        print("\n📊 Position Summary (League-Aware):")
        print("  Position | Starters | Bench | Total | Status")
        print("  ---------|----------|-------|-------|--------")

        eligible_positions = self.get_eligible_positions()
        for pos in ["QB", "RB", "WR", "TE", "K", "D/ST"]:
            # Skip positions not in this league
            if pos not in eligible_positions:
                continue
                
            data = summary[pos]

            # League-aware target determination
            if pos == "QB" and self.league_config and self.league_config.has_qb_flex:
                target = 3
            elif pos in ["RB", "WR"]:
                target = 5
            elif pos == "TE":
                target = 2
            else:
                target = 1

            if data["total"] > 0 or target > 0:
                # Special status for deferred positions
                if self.should_defer_position(pos) and data["total"] == 0:
                    status = "Wait"
                elif data["total"] >= target:
                    status = "✓"
                else:
                    status = f"Need {target - data['total']}"

                print(
                    f"  {pos:8} | {data['starters']:^8} | {data['bench']:^5} | {data['total']:^5} | {status}"
                )


class SuperflexDraftManager:
    """
    Complete draft tool combining ESPN league data with ADP values
    """

    def __init__(self, espn_connector, adp_csv_path: str):
        self.espn = espn_connector
        self.settings = self.espn.get_league_settings()

        # Initialize roster slot manager
        self.roster_manager = RosterSlotManager(self.settings)

        # Load and adjust ADP data
        self.players = self.load_and_adjust_adp(adp_csv_path)

        # Track draft state
        self.drafted_players = []
        self.my_team = []
        self.current_pick = 1

        # State persistence setup
        league_name_clean = self.settings["name"].replace(" ", "_").replace("/", "_")
        self.state_file = f"draft_state_{league_name_clean}.json"
        self.backup_dir = Path("draft_backups")
        self.backup_dir.mkdir(exist_ok=True)

        print(f"✓ Loaded {len(self.players)} players")
        print(f"✓ Adjusted for Superflex scoring")
        print(f"✓ Roster slots initialized: {self._get_roster_slots_summary()}")

        # Auto-load if state file exists
        self.load_draft_state()

    def _get_roster_slots_summary(self):
        """
        Get summary of roster configuration
        """
        config = self.settings["roster_slots"]
        slots = []

        # Starting positions
        for pos in ["QB", "RB", "WR", "TE", "FLEX", "OP", "K", "D/ST"]:
            if pos in config and config[pos] > 0:
                slots.append(f"{config[pos]}{pos}")

        # Bench
        if "BENCH" in config:
            slots.append(f"{config['BENCH']}BN")

        return ", ".join(slots)

    def get_current_round(self) -> int:
        """
        Calculate the current round based on pick number and league size
        """
        num_teams = self.settings.get("num_teams", 10)
        return ((self.current_pick - 1) // num_teams) + 1

    def get_total_rounds(self) -> int:
        """
        Calculate total number of rounds in the draft
        """
        total_roster_spots = sum(self.roster_manager.roster_config.values())
        return total_roster_spots

    def is_late_round(self) -> bool:
        """
        Determine if we're in the last 2 rounds of the draft
        """
        current_round = self.get_current_round()
        total_rounds = self.get_total_rounds()
        return current_round >= (total_rounds - 1)  # Last 2 rounds

    def load_and_adjust_adp(self, csv_path: str) -> pd.DataFrame:
        """
        Load ADP data and adjust for Superflex with FIXED calculations
        """
        # Load your ADP CSV
        df = pd.read_csv(csv_path)

        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(" ", "_")

        # Extract position from POS column (e.g., "WR1" -> "WR")
        df["position"] = df["pos"].str.extract(r"([A-Z]+)")

        # Standardize defense positions
        df.loc[df["position"].isin(["D", "DST"]), "position"] = "D/ST"

        # Use ESPN column for primary ADP
        df["adp_standard"] = df["espn"].fillna(df["avg"])

        # FIXED: Proper Superflex ADP adjustment
        def adjust_qb_adp(row):
            if row["position"] != "QB":
                return row["adp_standard"] + 5  # Non-QBs move down slightly

            orig_adp = row["adp_standard"]

            # Scale QB ADPs based on tier
            if orig_adp <= 50:
                # Elite QBs move up significantly
                new_adp = orig_adp * 0.3
            elif orig_adp <= 80:
                # Mid QBs move up moderately
                new_adp = orig_adp * 0.5
            else:
                # Late QBs move up slightly
                new_adp = orig_adp * 0.7

            return max(1, min(new_adp, orig_adp))

        df["adp_superflex"] = df.apply(adjust_qb_adp, axis=1)

        # Add IMPROVED projections
        df["projected_points"] = self.estimate_projections(df)

        # Add FIXED tier assignments
        df["tier"] = self.assign_tiers(df)

        # Sort by Superflex ADP
        df = df.sort_values("adp_superflex").reset_index(drop=True)

        return df

    def estimate_projections(self, df: pd.DataFrame) -> pd.Series:
        """
        More realistic projection estimates
        """
        projections = []
        for _, row in df.iterrows():
            pos = row["position"]
            adp = row["adp_standard"]

            if pos == "QB":
                # Flatter curve for QBs
                if adp <= 10:
                    proj = 400 - (adp * 3)
                elif adp <= 30:
                    proj = 370 - (adp * 2)
                elif adp <= 60:
                    proj = 320 - (adp * 1)
                else:
                    proj = 250 - (adp * 0.5)

            elif pos == "RB":
                # Steeper early, flatter later
                if adp <= 5:
                    proj = 300 - (adp * 8)
                elif adp <= 15:
                    proj = 270 - (adp * 4)
                elif adp <= 30:
                    proj = 230 - (adp * 2)
                else:
                    proj = 180 - (adp * 0.5)

            elif pos == "WR":
                # Similar to RB but slightly flatter
                if adp <= 5:
                    proj = 280 - (adp * 6)
                elif adp <= 15:
                    proj = 250 - (adp * 3)
                elif adp <= 30:
                    proj = 210 - (adp * 1.5)
                else:
                    proj = 170 - (adp * 0.5)

            elif pos == "TE":
                if adp <= 10:
                    proj = 220 - (adp * 5)
                elif adp <= 30:
                    proj = 180 - (adp * 2)
                else:
                    proj = 120
            else:
                proj = 100

            projections.append(max(50, proj))

        return pd.Series(projections)

    def assign_tiers(self, df: pd.DataFrame) -> pd.Series:
        """
        FIXED tier assignment
        """
        tiers = []

        # First, sort by position and ADP
        for idx, row in df.iterrows():
            pos = row["position"]

            # Get all players at this position, sorted by ADP
            pos_players = (
                df[df["position"] == pos]
                .sort_values("adp_superflex")
                .reset_index(drop=True)
            )

            # Find this player's rank at position
            player_name = row["player"]
            pos_rank = (
                pos_players[pos_players["player"] == player_name].index[0] + 1
                if len(pos_players[pos_players["player"] == player_name]) > 0
                else 99
            )

            # Assign tier based on positional rank
            if pos_rank <= 4:
                tier = 1
            elif pos_rank <= 10:
                tier = 2
            elif pos_rank <= 20:
                tier = 3
            elif pos_rank <= 35:
                tier = 4
            else:
                tier = 5

            tiers.append(tier)

        return pd.Series(tiers, index=df.index)

    def get_available_players(self) -> pd.DataFrame:
        """
        Get all undrafted players
        """
        return self.players[~self.players["player"].isin(self.drafted_players)]

    def calculate_value_score(self, player_row, current_pick: int) -> float:
        """
        Calculate how much value a player represents
        """
        # Basic value: how far past ADP
        adp_value = current_pick - player_row["adp_superflex"]

        # Tier bonus (higher tier = more valuable)
        tier_bonus = (6 - player_row["tier"]) * 2

        # Positional scarcity bonus
        position = player_row["position"]
        available = self.get_available_players()
        pos_available = available[available["position"] == position]

        scarcity_bonus = 0
        if position == "QB" and len(pos_available) < 15:
            scarcity_bonus = 10
        elif position == "RB" and len(pos_available) < 20:
            scarcity_bonus = 5

        return adp_value + tier_bonus + scarcity_bonus

    def calculate_team_need_score(self, player_row) -> float:
        """
        Enhanced need score with K/D/ST suppression until late rounds
        """
        position = player_row["position"]
        needs = self.roster_manager.get_needs_analysis()
        summary = self.roster_manager.get_position_summary()

        # NEW: Suppress K and D/ST recommendations until late rounds
        if position in ["K", "D", "DST", "D/ST"]:
            if not self.is_late_round():
                # Return negative score to heavily deprioritize K/D/ST early
                return -50  # This will push them way down the recommendation list

        # Check critical needs first (unfilled starters)
        for slot in needs["critical"]:
            # Direct position match (but not K/D/ST early)
            slot_base = slot.replace("1", "").replace("2", "").replace("3", "")
            if slot_base == position:
                if position not in ["K", "D/ST"] or self.is_late_round():
                    return 20  # Highest priority
            # Special case for defenses (handle all formats) - only in late rounds
            if position in ["D", "DST", "D/ST"] and slot.startswith("D/ST"):
                if self.is_late_round():
                    return 20

        # Check flex/OP eligibility for important needs
        if position in ["RB", "WR", "TE", "QB"]:
            for slot in needs["important"]:
                if slot.startswith("FLEX") and position in ["RB", "WR", "TE"]:
                    return 10
                if slot.startswith("OP"):  # Any offensive position
                    return 10

        # Check if we've hit position caps
        display_position = position
        if position in ["D", "DST"]:
            display_position = "D/ST"

        pos_data = summary.get(display_position, {"total": 0})

        position_targets = {
            "QB": 3 if self.settings["is_superflex"] else 2,
            "RB": 6,
            "WR": 7,
            "TE": 2,
            "K": 1,
            "D": 1,
            "DST": 1,
            "D/ST": 1,
        }

        target = position_targets.get(position, 0)
        if pos_data["total"] >= target:
            return 0  # No need

        # Calculate depth need
        remaining_need = target - pos_data["total"]
        return min(5, remaining_need * 2)  # Depth picks worth less

    def get_draft_recommendation(self, num_recommendations: int = 5) -> pd.DataFrame:
        """
        Get top recommended picks with smart position balancing and K/D/ST filtering
        """
        available = self.get_available_players()

        if available.empty:
            return pd.DataFrame()

        # NEW: Filter out K and D/ST early in the draft
        if not self.is_late_round():
            # Remove K and D/ST from available players for recommendations
            available = available[
                ~available["position"].isin(["K", "D", "DST", "D/ST"])
            ]

        # Calculate scores for each player
        recommendations = []
        for _, player in available.iterrows():
            value_score = self.calculate_value_score(player, self.current_pick)
            need_score = self.calculate_team_need_score(player)

            # ENHANCED: Boost need weighting when critical needs exist
            needs = self.roster_manager.get_needs_analysis()

            # Filter out K/D/ST from critical needs early
            if not self.is_late_round():
                critical_skill_needs = [
                    slot
                    for slot in needs["critical"]
                    if not slot.startswith(("K", "D/ST"))
                ]
            else:
                critical_skill_needs = needs["critical"]

            if critical_skill_needs:
                # More emphasis on need when we have unfilled starters
                total_score = (value_score * 0.4) + (need_score * 0.6)
            else:
                # Standard weighting when just looking for depth
                total_score = (value_score * 0.6) + (need_score * 0.4)

            recommendations.append(
                {
                    "player": player["player"],
                    "position": player["position"],
                    "team": player["team"],
                    "tier": player["tier"],
                    "adp": player["adp_superflex"],
                    "projected": player["projected_points"],
                    "value_score": round(value_score, 1),
                    "need_score": round(need_score, 1),
                    "total_score": round(total_score, 1),
                    "pick_value": (
                        "GREAT"
                        if value_score > 10
                        else "GOOD" if value_score > 5 else "FAIR"
                    ),
                }
            )

        df_rec = pd.DataFrame(recommendations)
        df_rec = df_rec.sort_values("total_score", ascending=False)

        # ENHANCED: Smart position balancing for recommendations
        return self._balance_position_recommendations(df_rec, num_recommendations)

    def _balance_position_recommendations(
        self, df_rec: pd.DataFrame, num_recommendations: int
    ) -> pd.DataFrame:
        """
        Balance recommendations to avoid over-showing saturated positions
        """
        if df_rec.empty:
            return df_rec

        summary = self.roster_manager.get_position_summary()
        needs = self.roster_manager.get_needs_analysis()

        # Identify over-saturated positions
        position_targets = {
            "QB": 3 if self.settings["is_superflex"] else 2,
            "RB": 6,
            "WR": 7,
            "TE": 2,
            "K": 1,
            "D/ST": 1,
        }

        saturated_positions = set()
        for pos, target in position_targets.items():
            pos_data = summary.get(pos, {"total": 0})
            if pos_data["total"] >= target:
                saturated_positions.add(pos)

        # If we have critical needs, limit saturated positions more aggressively
        max_saturated_recs = 1 if needs["critical"] else 2

        balanced_recs = []
        saturated_count = {}

        for _, rec in df_rec.iterrows():
            pos = rec["position"]
            if pos in ["D", "DST"]:
                pos = "D/ST"

            # If position is saturated, limit how many we show
            if pos in saturated_positions:
                current_count = saturated_count.get(pos, 0)
                if current_count >= max_saturated_recs:
                    continue  # Skip this recommendation
                saturated_count[pos] = current_count + 1

            balanced_recs.append(rec)

            # Stop when we have enough recommendations
            if len(balanced_recs) >= num_recommendations:
                break

        return pd.DataFrame(balanced_recs)

    def draft_player(self, player_name: str, team: str = "my_team"):
        """
        ENHANCED: Mark a player as drafted with fuzzy matching and roster slots
        """
        # Clean up the input
        player_name = player_name.strip()

        # Filter out rows with NaN player names
        valid_players = self.players[self.players["player"].notna()].copy()

        # Try exact match first (case insensitive)
        exact_matches = valid_players[
            valid_players["player"].str.lower() == player_name.lower()
        ]

        if not exact_matches.empty:
            player_matches = exact_matches
        else:
            # ENHANCED fuzzy matching with position awareness
            player_matches = self._smart_fuzzy_match(player_name, valid_players)
            if player_matches is None:
                return False

        actual_player_name = player_matches.iloc[0]["player"]

        # Check if already drafted
        if actual_player_name in self.drafted_players:
            print(f"⚠️ {actual_player_name} has already been drafted!")
            return False

        self.drafted_players.append(actual_player_name)

        if team == "my_team":
            player_data = player_matches.iloc[0]

            # Fix defense position handling
            position = player_data["position"]
            if position in ["D", "DST"]:
                position = "D/ST"

            # Add to roster manager
            slot_filled = self.roster_manager.add_player(
                actual_player_name, position, self.current_pick
            )

            # CRITICAL FIX: Check if roster is full
            if slot_filled is None:
                print(f"❌ No roster slots available for {actual_player_name}")
                print("⚠️ Your roster is full - this pick cannot be added!")
                # Undo the drafted player addition
                self.drafted_players.remove(actual_player_name)
                return False

            self.my_team.append(
                {
                    "player": actual_player_name,
                    "position": position,
                    "pick": self.current_pick,
                    "slot": slot_filled,
                }
            )
            print(
                f"✓ Drafted {actual_player_name} ({position}) to {slot_filled} at pick {self.current_pick}"
            )
        else:
            player_data = player_matches.iloc[0]
            position = player_data["position"]
            if position in ["D", "DST"]:
                position = "D/ST"
            print(
                f"✓ {actual_player_name} ({position}) drafted by another team at pick {self.current_pick}"
            )

        self.current_pick += 1

        # Auto-save after each successful pick
        self.save_draft_state()

        return True

    def _smart_fuzzy_match(self, player_name: str, valid_players: pd.DataFrame):
        """
        Enhanced fuzzy matching with position awareness and better defense handling
        """
        search_lower = player_name.lower()

        # Check if this looks like a defense search
        is_defense_search = self._is_defense_search(search_lower, valid_players)

        if is_defense_search:
            # Only search among defenses
            defense_players = valid_players[
                valid_players["position"].isin(["D", "DST", "D/ST"])
            ]
            if not defense_players.empty:
                defense_names = defense_players["player"].str.lower().tolist()
                close_matches = difflib.get_close_matches(
                    search_lower,
                    defense_names,
                    n=3,
                    cutoff=0.3,  # Lower cutoff for defenses
                )

                if close_matches:
                    if len(close_matches) == 1:
                        matched_name = close_matches[0]
                        player_matches = defense_players[
                            defense_players["player"].str.lower() == matched_name
                        ]
                        print(f"📝 Auto-matched to: {player_matches.iloc[0]['player']}")
                        return player_matches
                    else:
                        print(f"Multiple defense matches for '{player_name}':")
                        for match in close_matches:
                            player_info = defense_players[
                                defense_players["player"].str.lower() == match
                            ].iloc[0]
                            print(
                                f"  - {player_info['player']} ({player_info['position']})"
                            )
                        return None

        # Regular fuzzy matching for non-defense players
        all_names = valid_players["player"].str.lower().tolist()
        close_matches = difflib.get_close_matches(
            search_lower, all_names, n=5, cutoff=0.6
        )

        if not close_matches:
            print(f"❌ No matches found for '{player_name}'")
            # Show suggestions based on first few letters
            suggestions = valid_players[
                valid_players["player"]
                .str.lower()
                .str.startswith(
                    search_lower[:3] if len(search_lower) >= 3 else search_lower
                )
            ].head(3)
            if not suggestions.empty:
                print("Did you mean:")
                for _, p in suggestions.iterrows():
                    print(f"  - {p['player']} ({p['position']}, {p['team']})")
            return None

        if len(close_matches) == 1:
            # Auto-select if only one close match
            matched_name = close_matches[0]
            player_matches = valid_players[
                valid_players["player"].str.lower() == matched_name
            ]

            # Safety check - don't auto-match if positions are completely different
            matched_player = player_matches.iloc[0]
            if self._is_reasonable_match(player_name, matched_player):
                print(f"📝 Auto-matched to: {matched_player['player']}")
                return player_matches
            else:
                print(
                    f"❌ '{player_name}' doesn't seem to match '{matched_player['player']}' ({matched_player['position']})"
                )
                return None
        else:
            # Show options
            print(f"Multiple matches for '{player_name}':")
            for match in close_matches:
                player_info = valid_players[
                    valid_players["player"].str.lower() == match
                ].iloc[0]
                print(
                    f"  - {player_info['player']} ({player_info['position']}, {player_info['team']})"
                )
            return None

    def _is_defense_search(self, search_term: str, valid_players: pd.DataFrame) -> bool:
        """
        Dynamically determine if search term is looking for a defense
        """
        # Obvious defense keywords
        defense_keywords = ["defense", "dst", "d/st", "def"]
        if any(keyword in search_term for keyword in defense_keywords):
            return True

        # Get all defense team names from the actual data
        defense_players = valid_players[
            valid_players["position"].isin(["D", "DST", "D/ST"])
        ]
        if defense_players.empty:
            return False

        # Extract team names and cities from defense player names
        defense_terms = set()
        for defense_name in defense_players["player"].str.lower():
            # Split defense names and extract potential team identifiers
            words = (
                defense_name.replace("defense", "")
                .replace("dst", "")
                .replace("d/st", "")
                .split()
            )
            for word in words:
                if len(word) > 2:  # Skip short words like "d", "st"
                    defense_terms.add(word)

        # Check if search term matches any actual defense team identifier
        search_words = search_term.split()
        for word in search_words:
            if word in defense_terms:
                return True

        # Check for partial matches with defense terms
        for defense_term in defense_terms:
            if len(search_term) >= 4 and (
                search_term in defense_term or defense_term in search_term
            ):
                return True

        return False

    def _is_reasonable_match(self, search_term: str, matched_player: pd.Series) -> bool:
        """
        Check if a fuzzy match makes sense (prevent random mismatches)
        """
        search_lower = search_term.lower()
        player_name_lower = matched_player["player"].lower()
        position = matched_player["position"]

        # If already determined to be a defense search, only allow defense matches
        if self._is_defense_search(search_lower, pd.DataFrame([matched_player])):
            return position in ["D", "DST", "D/ST"]

        # If search term and match share at least 3 characters, probably OK
        common_chars = set(search_lower) & set(player_name_lower)
        if len(common_chars) >= 3:
            return True

        # Otherwise be conservative
        return False

    def drop_player(self, player_name: str):
        """
        Drop a player from your roster to make room
        """
        player_name = player_name.strip()

        # Find player on your team
        player_to_drop = None
        for i, player in enumerate(self.my_team):
            if player["player"].lower() == player_name.lower():
                player_to_drop = (i, player)
                break

        if not player_to_drop:
            # Try fuzzy matching
            team_names = [p["player"].lower() for p in self.my_team]
            close_matches = difflib.get_close_matches(
                player_name.lower(), team_names, n=3, cutoff=0.6
            )

            if not close_matches:
                print(f"❌ '{player_name}' not found on your team")
                return False
            elif len(close_matches) == 1:
                # Auto-match
                matched_name = close_matches[0]
                for i, player in enumerate(self.my_team):
                    if player["player"].lower() == matched_name:
                        player_to_drop = (i, player)
                        print(f"📝 Auto-matched to: {player['player']}")
                        break
            else:
                print(f"Multiple matches for '{player_name}':")
                for match in close_matches:
                    for player in self.my_team:
                        if player["player"].lower() == match:
                            print(
                                f"  - {player['player']} ({player['position']}) at {player['slot']}"
                            )
                return False

        if not player_to_drop:
            return False

        idx, player = player_to_drop

        # Remove from roster manager
        slot_name = player["slot"]
        if slot_name and slot_name in self.roster_manager.roster:
            self.roster_manager.roster[slot_name] = None

        # Remove from my_team
        dropped_player = self.my_team.pop(idx)

        # Remove from drafted players so they can be re-drafted
        if dropped_player["player"] in self.drafted_players:
            self.drafted_players.remove(dropped_player["player"])

        print(
            f"✓ Dropped {dropped_player['player']} ({dropped_player['position']}) from {slot_name}"
        )

        # Auto-save state
        self.save_draft_state()
        return True

    def undo_last_pick(self):
        """
        Undo your most recent draft pick (like many fantasy platforms allow)
        """
        if not self.my_team:
            print("❌ No picks to undo!")
            return False

        # Get your most recent pick
        last_pick = self.my_team[-1]

        # Confirm it's actually your most recent overall pick
        if last_pick["pick"] != self.current_pick - 1:
            print("❌ Can only undo your most recent pick!")
            print(
                f"Your last pick was #{last_pick['pick']}, but current pick is #{self.current_pick}"
            )
            return False

        # Remove from roster manager
        slot_name = last_pick["slot"]
        if slot_name and slot_name in self.roster_manager.roster:
            self.roster_manager.roster[slot_name] = None

        # Remove from my_team
        undone_pick = self.my_team.pop()

        # Remove from drafted players (makes them available again)
        if undone_pick["player"] in self.drafted_players:
            self.drafted_players.remove(undone_pick["player"])

        # Decrement pick counter
        self.current_pick -= 1

        print(
            f"↩️ Undid pick #{undone_pick['pick']}: {undone_pick['player']} ({undone_pick['position']})"
        )
        print(f"🔄 {undone_pick['player']} is now available to draft again")

        # Auto-save state
        self.save_draft_state()
        return True

    def save_draft_state(self, manual_save=False):
        """
        Save current draft state to JSON file with timestamp backup
        """
        state = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "league_info": {
                "name": self.settings["name"],
                "league_id": self.espn.league_id,
                "year": self.espn.year,
            },
            "draft_progress": {
                "current_pick": self.current_pick,
                "drafted_players": self.drafted_players.copy(),
                "my_team": self.my_team.copy(),
            },
            "roster_state": {
                "roster": {k: v for k, v in self.roster_manager.roster.items()},
                "roster_config": self.roster_manager.roster_config,
            },
        }

        try:
            # Save main state file
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=2)

            # Create timestamped backup
            backup_file = (
                self.backup_dir / f"draft_backup_{time.strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(backup_file, "w") as f:
                json.dump(state, f, indent=2)

            # Clean old backups (keep last 10)
            self._cleanup_old_backups()

            if manual_save:
                print(f"✓ Manual save completed (Pick {self.current_pick})")

        except Exception as e:
            print(f"⚠️ Save failed: {e}")

    def load_draft_state(self):
        """
        Load draft state from file if it exists
        """
        if not Path(self.state_file).exists():
            print("📝 Starting fresh draft")
            return False

        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)

            # Restore draft progress
            self.current_pick = state["draft_progress"]["current_pick"]
            self.drafted_players = state["draft_progress"]["drafted_players"]
            self.my_team = state["draft_progress"]["my_team"]

            # Restore roster state
            self.roster_manager.roster = state["roster_state"]["roster"]

            # Convert None strings back to None (JSON limitation)
            for slot, player in self.roster_manager.roster.items():
                if player == "null" or player == "None":
                    self.roster_manager.roster[slot] = None

            saved_time = state["timestamp"]
            print(f"✓ Draft state loaded from {saved_time}")
            print(f"✓ Resuming at pick {self.current_pick}")
            print(f"✓ {len(self.drafted_players)} players already drafted")
            print(f"✓ {len(self.my_team)} players on your team")

            return True

        except Exception as e:
            print(f"⚠️ Failed to load draft state: {e}")
            print("📝 Starting fresh draft")
            return False

    def _cleanup_old_backups(self):
        """
        Keep only the 10 most recent backup files
        """
        try:
            backup_files = list(self.backup_dir.glob("draft_backup_*.json"))
            backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

            # Remove files beyond the 10 most recent
            for old_file in backup_files[10:]:
                old_file.unlink()

        except Exception as e:
            pass  # Silent fail on cleanup

    def show_draft_status(self):
        """
        Show comprehensive draft status
        """
        print("\n📊 DRAFT STATUS SUMMARY")
        print("=" * 40)
        print(f"Current Pick: {self.current_pick}")
        print(f"Round: {self.get_current_round()}/{self.get_total_rounds()}")
        print(f"Players Drafted: {len(self.drafted_players)}")
        print(f"My Team Size: {len(self.my_team)}")

        if self.my_team:
            print(f"\nMy Picks:")
            for player in self.my_team:
                print(
                    f"  Pick {player['pick']}: {player['player']} ({player['position']}) → {player.get('slot', 'N/A')}"
                )

        # Show most recent drafts
        if len(self.drafted_players) > 0:
            recent_picks = self.drafted_players[-5:]  # Last 5 picks
            print(f"\nRecent Picks: {', '.join(recent_picks)}")

    def load_from_backup(self):
        """
        Interactive backup file selection
        """
        backup_files = list(self.backup_dir.glob("draft_backup_*.json"))
        if not backup_files:
            print("❌ No backup files found")
            return

        backup_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)

        print("\n📁 Available Backup Files:")
        for i, backup in enumerate(backup_files[:5], 1):
            timestamp = time.strftime(
                "%Y-%m-%d %H:%M:%S", time.localtime(backup.stat().st_mtime)
            )
            print(f"  {i}. {backup.name} ({timestamp})")

        try:
            choice = input("\nEnter backup number (1-5) or 'cancel': ").strip()
            if choice.lower() == "cancel":
                return

            backup_idx = int(choice) - 1
            if 0 <= backup_idx < len(backup_files[:5]):
                selected_backup = backup_files[backup_idx]

                # Load the backup
                with open(selected_backup, "r") as f:
                    state = json.load(f)

                # Restore state (same logic as load_draft_state)
                self.current_pick = state["draft_progress"]["current_pick"]
                self.drafted_players = state["draft_progress"]["drafted_players"]
                self.my_team = state["draft_progress"]["my_team"]
                self.roster_manager.roster = state["roster_state"]["roster"]

                print(f"✓ Loaded backup from {selected_backup.name}")

        except (ValueError, IndexError, KeyError) as e:
            print(f"❌ Invalid selection: {e}")

    def show_draft_board(self):
        """
        ENHANCED: Display current draft state with roster slots and round info
        """
        print("\n" + "=" * 60)
        print(
            f"PICK {self.current_pick} - ROUND {self.get_current_round()}/{self.get_total_rounds()} - RECOMMENDATIONS"
        )
        print("=" * 60)

        # Show K/D/ST strategy message if not in late rounds
        if not self.is_late_round():
            rounds_until_kd = self.get_total_rounds() - self.get_current_round()
            print(
                f"\n📌 Strategy: Focusing on skill positions (K/D/ST in {rounds_until_kd} rounds)"
            )

        # Show roster with slots filled
        self._display_roster_slots()

        # Show roster capacity
        self.show_roster_capacity()

    def show_roster_capacity(self):
        """
        Show current roster capacity info
        """
        total_slots = sum(self.roster_manager.roster_config.values())
        filled_slots = len(
            [p for p in self.roster_manager.roster.values() if p is not None]
        )

        print(f"\n📊 Roster Capacity: {filled_slots}/{total_slots} slots filled")

        if filled_slots >= total_slots:
            print("⚠️ ROSTER FULL - No more picks can be added!")
        else:
            remaining = total_slots - filled_slots
            print(f"✅ {remaining} slots remaining")

        # Show needs analysis with K/D/ST filtering
        needs = self.roster_manager.get_needs_analysis()

        # Filter K/D/ST from critical needs if not late round
        if not self.is_late_round():
            critical_filtered = [
                slot for slot in needs["critical"] if not slot.startswith(("K", "D/ST"))
            ]
            deferred_needs = [
                slot for slot in needs["critical"] if slot.startswith(("K", "D/ST"))
            ]
        else:
            critical_filtered = needs["critical"]
            deferred_needs = []

        if critical_filtered:
            print("\n⚠️  Critical Needs (Starting Positions):")
            print(f"   {', '.join(critical_filtered)}")
        if deferred_needs:
            print("\n⏳ Deferred Needs (Will address in final rounds):")
            print(f"   {', '.join(deferred_needs)}")
        if needs["important"]:
            print("\n📊 Important Needs (Flex/OP):")
            print(f"   {', '.join(needs['important'])}")

        # Show recommendations
        recs = self.get_draft_recommendation()

        if not recs.empty:
            print("\n🎯 Top Recommendations (Best Value):")
            for _, rec in recs.iterrows():
                # Add indicator if fills critical need
                need_indicator = ""
                for slot in critical_filtered:  # Use filtered list
                    slot_base = slot.replace("1", "").replace("2", "").replace("3", "")
                    if rec["position"] == slot_base:
                        need_indicator = " ⭐ [FILLS STARTER NEED]"
                        break

                print(
                    f"\n{rec['player']} ({rec['position']}) - {rec['team']}{need_indicator}"
                )
                print(
                    f"  Tier: {rec['tier']} | ADP: {rec['adp']:.1f} | Proj: {rec['projected']:.0f}"
                )
                print(
                    f"  Value: {rec['value_score']} | Need: {rec['need_score']} | TOTAL: {rec['total_score']}"
                )
                print(f"  Verdict: {rec['pick_value']} VALUE")

        # Show best available at each needed position
        self._display_positional_needs()

        # Show position summary
        self._display_position_summary()

    def _display_positional_needs(self):
        """
        Display best available players at each needed position
        """
        needs = self.roster_manager.get_needs_analysis()
        available = self.get_available_players()

        if not needs["critical"] and not needs["important"]:
            return

        print("\n🔍 Best Available at Positional Needs:")
        print("-" * 50)

        # Get positions we actually need
        needed_positions = set()

        # Critical needs (starting positions)
        for slot in needs["critical"]:
            pos = slot.replace("1", "").replace("2", "").replace("3", "")
            # Skip K/D/ST if not in late rounds
            if not self.is_late_round() and pos in ["K", "D/ST"]:
                continue
            needed_positions.add(pos)

        # Important needs (FLEX/OP eligible positions)
        if needs["important"]:
            needed_positions.update(["RB", "WR", "TE", "QB"])

        # Show top 2-3 at each needed position
        for position in ["QB", "RB", "WR", "TE", "K", "D/ST"]:
            if position not in needed_positions:
                continue

            # Handle defense format variations
            if position == "D/ST":
                pos_players = available[
                    available["position"].isin(["D/ST", "D", "DST"])
                ].copy()
            else:
                pos_players = available[available["position"] == position].copy()

            if pos_players.empty:
                continue

            # Sort by ADP (best available first)
            pos_players = pos_players.sort_values("adp_superflex").head(3)

            # Determine if this is critical or important need
            is_critical = any(
                slot.replace("1", "").replace("2", "").replace("3", "") == position
                or (position == "D/ST" and slot.startswith("D/ST"))
                for slot in needs["critical"]
            )

            # Special handling for K/D/ST
            if position in ["K", "D/ST"] and not self.is_late_round():
                need_type = "⏳ DEFERRED"
            elif is_critical:
                need_type = "🚨 CRITICAL"
            else:
                need_type = "📋 FLEX/OP"

            print(f"\n{position} ({need_type}):")

            for i, (_, player) in enumerate(pos_players.iterrows()):
                # Calculate value for this pick
                value_score = self.calculate_value_score(player, self.current_pick)

                # Simple display
                print(
                    f"  {i+1}. {player['player']} - {player['team']} "
                    f"(ADP: {player['adp_superflex']:.1f}, Value: {value_score:+.1f})"
                )

    def _display_roster_slots(self):
        """
        Display current roster with slot assignments
        """
        print("\n📋 Current Roster:")

        roster = self.roster_manager.roster

        # Group by position type for display
        position_groups = {
            "QB": [],
            "RB": [],
            "WR": [],
            "TE": [],
            "FLEX": [],
            "OP": [],
            "K": [],
            "D/ST": [],
            "BENCH": [],
        }

        for slot_name, player in roster.items():
            for group in position_groups:
                if slot_name.startswith(group):
                    if player:
                        position_groups[group].append(f"{slot_name}: {player['name']}")
                    else:
                        position_groups[group].append(f"{slot_name}: [EMPTY]")
                    break

        # Display non-empty groups
        for group, slots in position_groups.items():
            if slots:
                if group == "BENCH":
                    # Only show filled bench spots
                    filled_bench = [s for s in slots if "[EMPTY]" not in s]
                    empty_bench = len(slots) - len(filled_bench)
                    if filled_bench or empty_bench:
                        print(f"\n  Bench ({len(filled_bench)}/{len(slots)}):")
                        for slot in filled_bench:
                            print(f"    {slot}")
                else:
                    if any(
                        "[EMPTY]" not in s for s in slots
                    ):  # Only show if at least one filled
                        print(f"\n  {group}:")
                        for slot in slots:
                            if "[EMPTY]" in slot:
                                # Mark K/D/ST differently if not late round
                                if group in ["K", "D/ST"] and not self.is_late_round():
                                    print(f"    {slot} ⏳")
                                else:
                                    print(f"    {slot} ⚠️")
                            else:
                                print(f"    {slot} ✓")

    def _display_position_summary(self):
        """
        Display position summary with starter/bench breakdown
        """
        summary = self.roster_manager.get_position_summary()

        print("\n📊 Position Summary:")
        print("  Position | Starters | Bench | Total | Status")
        print("  ---------|----------|-------|-------|--------")

        for pos in ["QB", "RB", "WR", "TE", "K", "D/ST"]:
            data = summary[pos]

            # Determine target and status
            if pos == "QB" and self.settings["is_superflex"]:
                target = 3
            elif pos in ["RB", "WR"]:
                target = 5
            elif pos == "TE":
                target = 2
            else:
                target = 1

            if data["total"] > 0 or target > 0:
                # Special status for K/D/ST if not late round
                if (
                    pos in ["K", "D/ST"]
                    and not self.is_late_round()
                    and data["total"] == 0
                ):
                    status = "Wait"
                elif data["total"] >= target:
                    status = "✓"
                else:
                    status = f"Need {target - data['total']}"

                print(
                    f"  {pos:8} | {data['starters']:^8} | {data['bench']:^5} | {data['total']:^5} | {status}"
                )


def display_ascii_banner():
    """
    Display the magnificent YO SOY DRAFT WIZARD banner
    """
    banner = """
██╗   ██╗ ██████╗     ███████╗ ██████╗ ██╗   ██╗
╚██╗ ██╔╝██╔═══██╗    ██╔════╝██╔═══██╗╚██╗ ██╔╝
 ╚████╔╝ ██║   ██║    ███████╗██║   ██║ ╚████╔╝ 
  ╚██╔╝  ██║   ██║    ╚════██║██║   ██║  ╚██╔╝  
   ██║   ╚██████╔╝    ███████║╚██████╔╝   ██║   
   ╚═╝    ╚═════╝     ╚══════╝ ╚═════╝    ╚═╝   

██████╗ ██████╗  █████╗ ███████╗████████╗
██╔══██╗██╔══██╗██╔══██╗██╔════╝╚══██╔══╝
██║  ██║██████╔╝███████║█████╗     ██║   
██║  ██║██╔══██╗██╔══██║██╔══╝     ██║   
██████╔╝██║  ██║██║  ██║██║        ██║   
╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝        ╚═╝   

██╗    ██╗██╗███████╗ █████╗ ██████╗ ██████╗ 
██║    ██║██║╚══███╔╝██╔══██╗██╔══██╗██╔══██╗
██║ █╗ ██║██║  ███╔╝ ███████║██████╔╝██║  ██║
██║███╗██║██║ ███╔╝  ██╔══██║██╔══██╗██║  ██║
╚███╔███╔╝██║███████╗██║  ██║██║  ██║██████╔╝
 ╚══╝╚══╝ ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ 
"""

    print("🧙‍♂️" + "=" * 56 + "🧙‍♂️")
    print(banner)
    print("🏈" + "=" * 56 + "🏈")
    print("        🔮 LEAGUE-AWARE DRAFT TOOL - ENHANCED VERSION 🔮")
    print("🧙‍♂️" + "=" * 56 + "🧙‍♂️")


def main():
    """
    Run the complete LEAGUE-AWARE draft tool
    """
    # Your credentials
    LEAGUE_ID = 537814
    YEAR = 2025
    SWID = "3C010FF1-0860-485F-BF82-17FC9D702287"
    ESPN_S2 = "AEA3Vq4gY3g0nEBJbDSO8wc%2F0VEYfotMhZQRRIECUt63Hn7kpx84dQfh3Skfg2ZmfjnBt6Z8RDRL2H0g8rcthTl6acbEKLe%2FMGaaMYM63cjankHE%2B182LQjoKN787%2Fzfm%2BrQt2BoIAed2A0ooHsPqTLv197%2BRJH1opJSPCPDxlhwOkvcAXKtE8NWXTEmDDu6VMsT6UasOb7LWJYQEtRJoaVlEcEKAvaEVzkkcRHHga%2BjRh8q8QN82LCUp4UzhE2Zs1VUDKSTrzTwqy31YaACHfD%2BjnN%2BVvxwXq9Y3Ef2SzDLSnPOKKZWYnuFzY%2FJiODbCes%3D"

    # Path to your ADP CSV
    ADP_CSV = "src/draft/data/FantasyPros_2025_Overall_ADP_Rankings.csv"

    # Display the magnificent banner
    display_ascii_banner()

    # ENHANCED: Initialize with League-Aware Draft Manager
    try:
        draft_tool = LeagueAwareDraftManager(LEAGUE_ID, YEAR, SWID, ESPN_S2, ADP_CSV)
    except Exception as e:
        print(f"⚠️ Failed to initialize league-aware mode: {e}")
        print("📝 Falling back to legacy mode...")
        # Fallback to legacy mode
        espn = ESPNSuperflexConnector(LEAGUE_ID, YEAR, SWID, ESPN_S2)
        draft_tool = SuperflexDraftManager(espn, ADP_CSV)

    # Interactive draft loop
    while True:
        draft_tool.show_draft_board()

        print("\n" + "-" * 60)
        print("Commands:")
        print("  d [player] - Draft player to your team")
        print("  o [player] - Mark player drafted by others")
        print("  undo       - Undo your last pick")
        print("  save       - Manual save current state")
        print("  load       - Load from backup file")
        print("  status     - Show draft progress summary")
        print("  q          - Quit")
        print("\nTip: State auto-saves after each pick!")

        command = input("\nEnter command: ").strip().lower()

        if command.startswith("d "):
            player = command[2:].strip()
            draft_tool.draft_player(player, "my_team")
        elif command.startswith("o "):
            player = command[2:].strip()
            draft_tool.draft_player(player, "other")
        elif command == "undo":
            draft_tool.undo_last_pick()
        elif command == "save":
            draft_tool.save_draft_state(manual_save=True)
        elif command == "load":
            draft_tool.load_from_backup()
        elif command == "status":
            draft_tool.show_draft_status()
        elif command == "q":
            # Final save before quitting
            draft_tool.save_draft_state(manual_save=True)
            break
        else:
            print(
                "Invalid command. Use 'd [player]', 'o [player]', 'undo', 'save', 'load', 'status', or 'q'"
            )

    print("\n✓ Draft session ended")
    if draft_tool.my_team:
        print("\nYour Final Roster:")
        for player in draft_tool.my_team:
            print(
                f"  Pick {player['pick']}: {player['player']} ({player['position']}) → {player.get('slot', 'N/A')}"
            )
        
        # Show final league context
        if hasattr(draft_tool, 'league_config') and draft_tool.league_config:
            print(f"\n🏈 League: {draft_tool.league_config.league_name}")
            print(f"📊 Format: {draft_tool.league_config.scoring_type}")
            if draft_tool.league_config.has_qb_flex:
                print(f"⚡ QB-Flex league with {draft_tool.league_config.total_qb_slots} QB slots")
            print(f"🎯 Positions drafted: {', '.join(sorted(set(p['position'] for p in draft_tool.my_team)))}")


if __name__ == "__main__":
    main()
