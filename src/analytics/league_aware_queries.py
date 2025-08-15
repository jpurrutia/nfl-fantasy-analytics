"""League-Aware Query Generator - Dynamic SQL based on league configuration."""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from src.utils.league_config import LeagueConfig
import logging

logger = logging.getLogger(__name__)


@dataclass
class QueryContext:
    """Context for generating league-aware queries."""
    league: LeagueConfig
    season: Optional[int] = None
    min_games: Optional[int] = None
    position_filter: Optional[str] = None
    
    def __post_init__(self):
        """Set defaults from league config."""
        if self.min_games is None:
            self.min_games = self.league.min_games


class LeagueAwareQueryBuilder:
    """Builds SQL queries that adapt to league configuration."""
    
    def __init__(self, league_config: LeagueConfig):
        self.league = league_config
    
    def _generate_position_thresholds(self, context: QueryContext) -> str:
        """Generate position-specific CASE statements for boom/bust/startable."""
        
        # Only include positions that exist in this league
        eligible_positions = context.league.all_eligible_positions
        
        cases = {
            'startable': [],
            'bust': [],
            'boom': []
        }
        
        for position in eligible_positions:
            # Skip flex positions in thresholds
            if position in ['FLEX', 'SUPERFLEX', 'OP', 'WR_TE', 'RB_WR']:
                continue
                
            for threshold_type in cases.keys():
                threshold = context.league.get_threshold(position, threshold_type)
                if threshold > 0:  # Only include if threshold is set
                    operator = '>=' if threshold_type in ['startable', 'boom'] else '<'
                    cases[threshold_type].append(
                        f"WHEN p.position = '{position}' AND perf.fantasy_points_ppr {operator} {threshold} THEN 1"
                    )
        
        # Build the complete CASE statements
        result = {}
        for threshold_type, case_list in cases.items():
            if case_list:
                result[threshold_type] = f"""
        SUM(CASE
            {chr(10).join('            ' + case for case in case_list)}
            ELSE 0
        END)"""
            else:
                result[threshold_type] = "0"  # No eligible positions
        
        return result
    
    def _generate_position_filter(self, context: QueryContext) -> str:
        """Generate WHERE clause for position filtering."""
        filters = []
        
        # Position filter
        if context.position_filter:
            filters.append(f"p.position = '{context.position_filter}'")
        else:
            # Only include positions relevant to this league
            eligible_positions = list(context.league.all_eligible_positions)
            if eligible_positions:
                position_list = "', '".join(eligible_positions)
                filters.append(f"p.position IN ('{position_list}')")
        
        # Season filter
        if context.season:
            filters.append(f"perf.season = {context.season}")
        
        # Basic data quality
        filters.append("perf.fantasy_points_ppr IS NOT NULL")
        
        return " AND ".join(filters) if filters else "1=1"
    
    def build_player_consistency_query(self, context: QueryContext) -> str:
        """Build the main player consistency query adapted to league settings."""
        
        thresholds = self._generate_position_thresholds(context)
        position_filter = self._generate_position_filter(context)
        
        # Adjust consistency score calculation for QB-eligible leagues
        consistency_divisor = 3
        if context.league.has_qb_flex and context.position_filter == 'QB':
            consistency_divisor = 2  # QBs are more valuable in superflex/OP leagues
        
        query = f"""
-- Player Consistency Metrics (League-Aware)
-- League: {context.league.league_name} ({context.league.scoring_type})
-- Positions: {', '.join(sorted(context.league.all_eligible_positions))}
-- QB-Eligible Flex: {context.league.has_qb_flex}

SELECT
  perf.player_id
  ,p.name
  ,p.team
  ,p.position
  ,perf.season
  ,COUNT(1) AS n_games
  ,SUM(perf.fantasy_points_ppr) AS total_pts
  
  -- Core performance metrics
  ,ROUND(AVG(perf.fantasy_points_ppr), 3) AS avg_performance_pts
  ,ROUND(STDDEV(perf.fantasy_points_ppr), 3) AS std_dev  
  ,ROUND(MIN(perf.fantasy_points_ppr), 3) AS floor
  ,ROUND(MAX(perf.fantasy_points_ppr), 3) AS ceiling

  -- Variability coefficient (lower = more consistent)
  ,ROUND(STDDEV(perf.fantasy_points_ppr) / NULLIF(ABS(AVG(perf.fantasy_points_ppr)), 0), 3) AS variability_coefficient

  -- Quartile-based consistency
  ,ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY perf.fantasy_points_ppr), 2) AS q1
  ,ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY perf.fantasy_points_ppr), 2) AS q3
  ,ROUND(
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY perf.fantasy_points_ppr) -
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY perf.fantasy_points_ppr), 2
  ) as iqr_range

  -- League-aware startable rate (% of games above position-specific threshold)
  ,ROUND(
      100.0 * {thresholds['startable']} / COUNT(1), 1
  ) AS startable_rate_pct

  -- League-aware bust rate (% of games below position-specific threshold)
  ,ROUND(
    100.0 * {thresholds['bust']} / COUNT(1), 1
  ) AS bust_rate_pct

  -- League-aware boom rate (% of games above position-specific threshold)
  ,ROUND(
    100.0 * {thresholds['boom']} / COUNT(1), 1
  ) AS boom_rate_pct

  -- Position-aware consistency score (0-100, higher = more consistent)
  ,ROUND(
    100 - (
      (STDDEV(perf.fantasy_points_ppr) / NULLIF(ABS(AVG(perf.fantasy_points_ppr)), 0.1)) * 100 / {consistency_divisor}
    ), 1
  ) AS consistency_score

  -- League context indicators
  ,'{context.league.get_position_scarcity(context.position_filter or 'UNKNOWN')}' AS position_scarcity
  ,{context.league.qb_value_multiplier} AS qb_value_multiplier
  
FROM bronze.nfl_player_performance perf
LEFT JOIN bronze.nfl_players p ON p.player_id = perf.player_id
WHERE {position_filter}
GROUP BY perf.player_id, p.team, p.name, p.position, perf.season
HAVING COUNT(1) >= {context.min_games}  -- Minimum games for statistical significance
ORDER BY 
  p.position,
  n_games DESC,
  consistency_score DESC
"""
        
        return query.strip()
    
    def build_position_summary_query(self, context: QueryContext) -> str:
        """Build query to summarize position availability in this league."""
        
        eligible_positions = list(context.league.all_eligible_positions)
        position_list = "', '".join(eligible_positions)
        
        query = f"""
-- League Position Summary
-- Shows available positions and their depth in this league format

SELECT 
    p.position,
    COUNT(DISTINCT p.player_id) as total_players,
    COUNT(DISTINCT CASE WHEN perf.n_games >= {context.min_games} THEN p.player_id END) as qualified_players,
    '{context.league.league_name}' as league_name,
    '{context.league.scoring_type}' as scoring_type,
    CASE 
        WHEN p.position = 'QB' AND {context.league.has_qb_flex} THEN 'QB_FLEX_ELIGIBLE'
        WHEN p.position IN ('RB', 'WR', 'TE') THEN 'FLEX_ELIGIBLE' 
        ELSE 'DEDICATED_ONLY'
    END as flex_eligibility
FROM bronze.nfl_players p
LEFT JOIN (
    SELECT 
        player_id,
        COUNT(1) as n_games
    FROM bronze.nfl_player_performance 
    WHERE fantasy_points_ppr IS NOT NULL
    GROUP BY player_id
) perf ON p.player_id = perf.player_id
WHERE p.position IN ('{position_list}')
GROUP BY p.position
ORDER BY 
    CASE p.position 
        WHEN 'QB' THEN 1
        WHEN 'RB' THEN 2  
        WHEN 'WR' THEN 3
        WHEN 'TE' THEN 4
        WHEN 'K' THEN 5
        WHEN 'DST' THEN 6
        ELSE 7
    END
"""
        
        return query.strip()
    
    def build_league_context_query(self) -> str:
        """Build query to show league configuration context."""
        
        flex_info = []
        for flex in self.league.flex_positions:
            eligible = ', '.join(sorted(flex.eligible_positions))
            flex_info.append(f"{flex.name}: {eligible}")
        
        flex_summary = ' | '.join(flex_info) if flex_info else 'None'
        
        query = f"""
-- League Configuration Context
SELECT 
    '{self.league.league_id}' as league_id,
    '{self.league.league_name}' as league_name,
    {self.league.league_size} as league_size,
    '{self.league.scoring_type}' as scoring_type,
    '{", ".join(sorted(self.league.scoring_positions))}' as scoring_positions,
    '{flex_summary}' as flex_positions,
    {self.league.has_qb_flex} as has_qb_flex,
    {self.league.has_kickers} as has_kickers,
    {self.league.has_defense} as has_defense,
    {self.league.total_qb_slots} as total_qb_slots,
    {self.league.qb_value_multiplier} as qb_value_multiplier,
    '{self.league.detection_source}' as detection_source
"""
        
        return query.strip()
    
    def get_eligible_positions_for_cli(self) -> List[str]:
        """Get list of positions that should appear in CLI choices."""
        return sorted(list(self.league.all_eligible_positions))
    
    def validate_position_choice(self, position: str) -> bool:
        """Validate that a position choice is valid for this league."""
        return position in self.league.all_eligible_positions


class QueryTemplateManager:
    """Manages query templates and caching for different league configurations."""
    
    def __init__(self):
        self._query_cache: Dict[str, str] = {}
    
    def get_cached_query(self, league_id: str, query_type: str) -> Optional[str]:
        """Get cached query for a league."""
        cache_key = f"{league_id}:{query_type}"
        return self._query_cache.get(cache_key)
    
    def cache_query(self, league_id: str, query_type: str, query: str) -> None:
        """Cache a generated query."""
        cache_key = f"{league_id}:{query_type}"
        self._query_cache[cache_key] = query
        logger.debug(f"Cached query: {cache_key}")
    
    def clear_cache(self, league_id: Optional[str] = None) -> None:
        """Clear query cache for a league or all leagues."""
        if league_id:
            keys_to_remove = [k for k in self._query_cache.keys() if k.startswith(f"{league_id}:")]
            for key in keys_to_remove:
                del self._query_cache[key]
        else:
            self._query_cache.clear()
        
        logger.info(f"Cleared query cache{'for league ' + league_id if league_id else ''}")


# Global query template manager instance
query_template_manager = QueryTemplateManager()