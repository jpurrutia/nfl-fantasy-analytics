-- Player Consistency Metrics (Static Version)
-- NOTE: This file is superseded by league-aware dynamic SQL generation
-- Use: SQLRunner.run_transformation("silver", "player_consistency") 
-- for automatic league-specific analytics
--
-- This static version assumes standard league with all positions
-- For custom league formats, the dynamic version auto-adapts

CREATE OR REPLACE VIEW silver.player_consistency AS
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

  -- Position-aware startable rate (% of games above threshold)
  ,ROUND(
      100.0 * SUM(CASE
        WHEN p.position IN ('QB') AND perf.fantasy_points_ppr >= 15 THEN 1
        WHEN p.position IN ('RB', 'WR') AND perf.fantasy_points_ppr >= 10 THEN 1
        WHEN p.position IN ('TE') AND perf.fantasy_points_ppr >= 8 THEN 1
        WHEN p.position IN ('K') AND perf.fantasy_points_ppr >= 5 THEN 1
        WHEN p.position IN ('DST') AND perf.fantasy_points_ppr >= 5 THEN 1
        ELSE 0
      END) / COUNT(1), 1
  ) AS startable_rate_pct

  -- Bust rate (% of games below floor threshold)
  ,ROUND(
    100.0 * SUM(CASE
      WHEN p.position IN ('QB') AND perf.fantasy_points_ppr < 10 THEN 1
      WHEN p.position IN ('RB', 'WR') AND perf.fantasy_points_ppr < 5 THEN 1
      WHEN p.position IN ('TE') AND perf.fantasy_points_ppr < 3 THEN 1
      WHEN p.position IN ('K', 'DST') AND perf.fantasy_points_ppr < 2 THEN 1
      ELSE 0
    END) / COUNT(1), 1
  ) AS bust_rate_pct

  -- Boom rate (% of games above ceiling threshold)
  ,ROUND(
    100.0 * SUM(CASE 
      WHEN p.position IN ('QB') AND perf.fantasy_points_ppr >= 25 THEN 1
      WHEN p.position IN ('RB', 'WR') AND perf.fantasy_points_ppr >= 20 THEN 1
      WHEN p.position IN ('TE') AND perf.fantasy_points_ppr >= 15 THEN 1
      WHEN p.position IN ('K', 'DST') AND perf.fantasy_points_ppr >= 12 THEN 1
      ELSE 0 
    END) / COUNT(1), 1
  ) AS boom_rate_pct

  -- Overall consistency score (0-100, higher = more consistent)
  ,ROUND(
    100 - (
      (STDDEV(perf.fantasy_points_ppr) / NULLIF(ABS(AVG(perf.fantasy_points_ppr)), 0.1)) * 100 / 3
    ), 1
  ) AS consistency_score
  
FROM bronze.nfl_player_performance perf
LEFT JOIN bronze.nfl_players p ON p.player_id = perf.player_id
WHERE perf.fantasy_points_ppr IS NOT NULL
GROUP BY perf.player_id, p.team, p.name, p.position, perf.season
HAVING COUNT(1) >= 8  -- Minimum games for statistical significance
ORDER BY 
  p.position,
  n_games DESC,
  consistency_score DESC;