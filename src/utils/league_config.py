"""League Configuration Module - Dynamic league-aware settings."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Set
import yaml
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class PositionThresholds:
    """Position-specific fantasy thresholds."""
    startable: float
    bust: float
    boom: float
    
    @classmethod
    def default_ppr(cls, position: str) -> "PositionThresholds":
        """Get default PPR thresholds for a position."""
        defaults = {
            "QB": cls(startable=15.0, bust=10.0, boom=25.0),
            "RB": cls(startable=10.0, bust=5.0, boom=20.0),
            "WR": cls(startable=10.0, bust=5.0, boom=20.0),
            "TE": cls(startable=8.0, bust=3.0, boom=15.0),
            "K": cls(startable=5.0, bust=2.0, boom=12.0),
            "DST": cls(startable=5.0, bust=2.0, boom=12.0),
        }
        return defaults.get(position, cls(startable=10.0, bust=5.0, boom=20.0))


@dataclass
class FlexPosition:
    """Represents a flex position with eligible position types."""
    name: str  # FLEX, SUPERFLEX, OP, etc.
    eligible_positions: Set[str]  # Which positions can fill this slot
    
    @classmethod
    def flex(cls) -> "FlexPosition":
        """Standard RB/WR/TE flex."""
        return cls("FLEX", {"RB", "WR", "TE"})
    
    @classmethod
    def superflex(cls) -> "FlexPosition":
        """Superflex - QB/RB/WR/TE."""
        return cls("SUPERFLEX", {"QB", "RB", "WR", "TE"})
    
    @classmethod
    def offensive_player(cls) -> "FlexPosition":
        """Offensive Player - All offensive positions including QB."""
        return cls("OP", {"QB", "RB", "WR", "TE"})
    
    @classmethod
    def wr_te_flex(cls) -> "FlexPosition":
        """WR/TE only flex."""
        return cls("WR_TE", {"WR", "TE"})
    
    @classmethod
    def rb_wr_flex(cls) -> "FlexPosition":
        """RB/WR only flex."""
        return cls("RB_WR", {"RB", "WR"})


@dataclass
class LeagueConfig:
    """Complete league configuration."""
    
    # Basic league info
    league_id: str
    league_name: str = "Unknown League"
    league_size: int = 12
    scoring_type: str = "PPR"  # PPR, HALF_PPR, STANDARD
    
    # Roster configuration
    roster_positions: Dict[str, int] = field(default_factory=dict)
    flex_positions: List[FlexPosition] = field(default_factory=list)
    
    # Position thresholds
    position_thresholds: Dict[str, PositionThresholds] = field(default_factory=dict)
    
    # Analytics settings
    min_games: int = 8
    stability_windows: List[int] = field(default_factory=lambda: [3, 5, 8])
    projection_method: str = "weighted"
    
    # Auto-detected from ESPN
    auto_detected: bool = False
    detection_source: str = "manual"
    
    def __post_init__(self):
        """Initialize derived fields."""
        # Parse flex positions from roster_positions
        if not self.flex_positions:
            self.flex_positions = self._parse_flex_positions()
        
        # Generate default thresholds for scoring positions
        if not self.position_thresholds:
            self.position_thresholds = {
                pos: PositionThresholds.default_ppr(pos)
                for pos in self.scoring_positions
            }
    
    def _parse_flex_positions(self) -> List[FlexPosition]:
        """Parse flex positions from roster configuration."""
        flex_positions = []
        
        for pos_name, count in self.roster_positions.items():
            if count > 0:
                if pos_name == "FLEX":
                    flex_positions.extend([FlexPosition.flex()] * count)
                elif pos_name == "SUPERFLEX":
                    flex_positions.extend([FlexPosition.superflex()] * count)
                elif pos_name == "OP":
                    flex_positions.extend([FlexPosition.offensive_player()] * count)
                elif pos_name == "WR_TE":
                    flex_positions.extend([FlexPosition.wr_te_flex()] * count)
                elif pos_name == "RB_WR":
                    flex_positions.extend([FlexPosition.rb_wr_flex()] * count)
        
        return flex_positions
    
    @property
    def scoring_positions(self) -> List[str]:
        """Get positions that score fantasy points (exclude bench/IR)."""
        return [
            pos for pos, count in self.roster_positions.items()
            if count > 0 and pos not in ["BENCH", "IR", "FLEX", "SUPERFLEX", "OP", "WR_TE", "RB_WR"]
        ]
    
    @property
    def all_eligible_positions(self) -> Set[str]:
        """Get all positions that can be started in this league."""
        positions = set(self.scoring_positions)
        
        # Add positions eligible for flex slots
        for flex in self.flex_positions:
            positions.update(flex.eligible_positions)
        
        return positions
    
    @property
    def has_kickers(self) -> bool:
        """Check if league uses kickers."""
        return "K" in self.scoring_positions
    
    @property
    def has_defense(self) -> bool:
        """Check if league uses defense."""
        return "DST" in self.scoring_positions or "D/ST" in self.scoring_positions
    
    @property
    def has_superflex(self) -> bool:
        """Check if league has superflex position."""
        return any(flex.name == "SUPERFLEX" for flex in self.flex_positions)
    
    @property
    def has_op(self) -> bool:
        """Check if league has offensive player position."""
        return any(flex.name == "OP" for flex in self.flex_positions)
    
    @property
    def has_qb_flex(self) -> bool:
        """Check if QBs can be started in flex positions."""
        return self.has_superflex or self.has_op
    
    @property
    def qb_value_multiplier(self) -> float:
        """
        Calculate QB value multiplier based on league format.
        Higher for superflex/OP leagues where QBs are more valuable.
        """
        if self.has_superflex or self.has_op:
            return 1.5  # QBs much more valuable
        return 1.0  # Standard QB value
    
    @property
    def total_qb_slots(self) -> int:
        """Calculate total QB slots including flex."""
        base_qb = self.roster_positions.get("QB", 0)
        flex_qb = sum(
            1 for flex in self.flex_positions 
            if "QB" in flex.eligible_positions
        )
        return base_qb + flex_qb
    
    def get_threshold(self, position: str, threshold_type: str) -> float:
        """Get specific threshold for a position."""
        if position in self.position_thresholds:
            threshold = getattr(self.position_thresholds[position], threshold_type)
            
            # Adjust QB thresholds for superflex/OP leagues
            if position == "QB" and self.has_qb_flex:
                if threshold_type == "startable":
                    return threshold * 0.8  # Lower bar for startable QBs
                elif threshold_type == "boom":
                    return threshold * 1.2  # Higher boom threshold
            
            return threshold
        return 0.0
    
    def get_position_scarcity(self, position: str) -> str:
        """
        Calculate position scarcity level.
        Affects how we value players at each position.
        """
        total_slots = self.roster_positions.get(position, 0)
        
        # Add flex slots where this position is eligible
        flex_slots = sum(
            1 for flex in self.flex_positions
            if position in flex.eligible_positions
        )
        
        total_startable = (total_slots + flex_slots) * self.league_size
        
        # Scarcity levels based on total startable slots
        if total_startable <= 12:
            return "VERY_SCARCE"  # QB in standard leagues
        elif total_startable <= 24:
            return "SCARCE"  # RB, TE
        elif total_startable <= 36:
            return "MODERATE"  # WR
        else:
            return "ABUNDANT"  # Most other positions
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "league_id": self.league_id,
            "league_name": self.league_name,
            "league_size": self.league_size,
            "scoring_type": self.scoring_type,
            "roster_positions": self.roster_positions,
            "flex_positions": [
                {
                    "name": flex.name,
                    "eligible_positions": list(flex.eligible_positions)
                }
                for flex in self.flex_positions
            ],
            "position_thresholds": {
                pos: {
                    "startable": thresh.startable,
                    "bust": thresh.bust,
                    "boom": thresh.boom
                }
                for pos, thresh in self.position_thresholds.items()
            },
            "min_games": self.min_games,
            "stability_windows": self.stability_windows,
            "projection_method": self.projection_method,
            "auto_detected": self.auto_detected,
            "detection_source": self.detection_source,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LeagueConfig":
        """Create from dictionary."""
        # Convert threshold dicts back to PositionThresholds objects
        position_thresholds = {}
        if "position_thresholds" in data:
            position_thresholds = {
                pos: PositionThresholds(**thresh_data)
                for pos, thresh_data in data["position_thresholds"].items()
            }
        
        # Convert flex position dicts back to FlexPosition objects
        flex_positions = []
        if "flex_positions" in data:
            flex_positions = [
                FlexPosition(flex_data["name"], set(flex_data["eligible_positions"]))
                for flex_data in data["flex_positions"]
            ]
        
        return cls(
            league_id=data["league_id"],
            league_name=data.get("league_name", "Unknown League"),
            league_size=data.get("league_size", 12),
            scoring_type=data.get("scoring_type", "PPR"),
            roster_positions=data.get("roster_positions", {}),
            flex_positions=flex_positions,
            position_thresholds=position_thresholds,
            min_games=data.get("min_games", 8),
            stability_windows=data.get("stability_windows", [3, 5, 8]),
            projection_method=data.get("projection_method", "weighted"),
            auto_detected=data.get("auto_detected", False),
            detection_source=data.get("detection_source", "manual"),
        )


class ConfigLoader:
    """Load and manage league configurations."""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = Path(config_path)
        self.cache_path = Path("config/league_cache.yaml")
    
    def load_base_config(self) -> Dict[str, Any]:
        """Load base configuration from config.yaml."""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_path}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Error parsing config file: {e}")
            return {}
    
    def load_manual_league_config(self) -> Optional[LeagueConfig]:
        """Load manually configured league settings."""
        config = self.load_base_config()
        league_config = config.get("league", {})
        
        if not league_config:
            return None
        
        # Extract league info
        league_id = config.get("espn", {}).get("league_id", "manual")
        roster_positions = league_config.get("roster_positions", {})
        
        return LeagueConfig(
            league_id=str(league_id),
            league_name="Manual Configuration",
            league_size=league_config.get("size", 12),
            scoring_type=league_config.get("scoring_type", "PPR"),
            roster_positions=roster_positions,
            min_games=config.get("analytics", {}).get("min_games", 8),
            projection_method=config.get("analytics", {}).get("projection_method", "weighted"),
            detection_source="manual"
        )
    
    def save_detected_config(self, league_config: LeagueConfig) -> None:
        """Save auto-detected league configuration to cache."""
        try:
            self.cache_path.parent.mkdir(exist_ok=True)
            with open(self.cache_path, 'w') as f:
                yaml.dump(league_config.to_dict(), f, default_flow_style=False)
            logger.info(f"Saved league configuration to {self.cache_path}")
        except Exception as e:
            logger.error(f"Error saving league config: {e}")
    
    def load_cached_config(self, league_id: str) -> Optional[LeagueConfig]:
        """Load cached league configuration."""
        try:
            if not self.cache_path.exists():
                return None
            
            with open(self.cache_path, 'r') as f:
                data = yaml.safe_load(f)
            
            if data and data.get("league_id") == league_id:
                return LeagueConfig.from_dict(data)
            
        except Exception as e:
            logger.error(f"Error loading cached config: {e}")
        
        return None
    
    def get_league_config(self, league_id: Optional[str] = None, force_detection: bool = False) -> LeagueConfig:
        """
        Get league configuration with fallback hierarchy:
        1. Auto-detected from ESPN (if league_id provided)
        2. Cached configuration (if exists)
        3. Manual configuration from config.yaml
        4. Default configuration
        """
        
        # Try cached config first (unless forcing detection)
        if league_id and not force_detection:
            cached = self.load_cached_config(league_id)
            if cached:
                logger.info(f"Using cached configuration for league {league_id}")
                return cached
        
        # Try ESPN auto-detection if league_id provided
        if league_id and (force_detection or not self.load_cached_config(league_id)):
            try:
                detector = ESPNLeagueDetector()
                detected_config = detector.detect_league_config(league_id)
                if detected_config:
                    # Save to cache for future use
                    self.save_detected_config(detected_config)
                    logger.info(f"✅ Auto-detected and cached league configuration")
                    return detected_config
                else:
                    logger.warning("ESPN auto-detection failed, falling back to manual config")
            except Exception as e:
                logger.warning(f"ESPN detection error: {e}, falling back to manual config")
        
        # Try manual config
        manual_config = self.load_manual_league_config()
        if manual_config:
            logger.info("Using manual configuration from config.yaml")
            return manual_config
        
        # Default fallback
        logger.info("Using default league configuration")
        return LeagueConfig(
            league_id=league_id or "default",
            league_name="Default League",
            roster_positions={
                "QB": 1, "RB": 2, "WR": 2, "TE": 1,
                "FLEX": 2, "DST": 1, "BENCH": 7
            },
            detection_source="default"
        )


class ESPNLeagueDetector:
    """Detect league settings from ESPN API."""
    
    def __init__(self, espn_connector=None):
        self.espn_connector = espn_connector
    
    def detect_league_config(self, league_id: str, swid: str = None, espn_s2: str = None) -> Optional[LeagueConfig]:
        """
        Detect league configuration from ESPN API.
        
        Args:
            league_id: ESPN league ID
            swid: Optional SWID cookie for private leagues
            espn_s2: Optional espn_s2 cookie for private leagues
            
        Returns:
            LeagueConfig object or None if detection fails
        """
        try:
            from src.connectors.espn_api import ESPNConnector
            
            # Create connector if not provided
            if not self.espn_connector:
                # Try to get credentials from config
                config_loader = ConfigLoader()
                config = config_loader.load_base_config()
                espn_config = config.get("espn", {})
                
                year = espn_config.get("year", 2024)
                swid = swid or espn_config.get("swid")
                espn_s2 = espn_s2 or espn_config.get("espn_s2")
                
                if not swid or not espn_s2:
                    logger.warning("ESPN credentials not found in config - public league access only")
                
                connector = ESPNConnector(
                    league_id=int(league_id),
                    year=year,
                    swid=swid,
                    espn_s2=espn_s2
                )
            else:
                connector = self.espn_connector
            
            # Test connection first
            if not connector.test_connection():
                logger.error("Failed to connect to ESPN API")
                return None
            
            # Detect league configuration
            config_dict = connector.detect_league_configuration()
            if not config_dict:
                logger.error("Failed to detect league configuration")
                return None
            
            # Convert to LeagueConfig object
            league_config = LeagueConfig.from_dict(config_dict)
            
            logger.info(f"✅ Successfully detected league configuration")
            logger.info(f"🏈 League: {league_config.league_name}")
            logger.info(f"📊 Positions: {', '.join(sorted(league_config.all_eligible_positions))}")
            
            if league_config.has_qb_flex:
                logger.info(f"⚡ QB-Flex league: {league_config.total_qb_slots} QB slots")
            if not league_config.has_kickers:
                logger.info(f"🚫 No kickers in this league")
            
            return league_config
            
        except ImportError:
            logger.error("ESPN connector not available")
            return None
        except Exception as e:
            logger.error(f"Error detecting league config from ESPN: {e}")
            return None