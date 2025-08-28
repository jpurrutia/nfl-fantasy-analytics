-- QB Week-to-Week Variance Analysis
-- Purpose: Measure within-season consistency for quarterbacks
-- Statistical basis: Coefficient of Variation (CV) = stddev/mean
-- Lower CV = more consistent performance

WITH weekly_performance AS (
  SELECT 
    passer_player_id,
    passer_player_name,
    season,
    week,
    -- Aggregate to week level first
    COUNT(*) as attempts,
    AVG(passing_yards) as avg_yards_per_attempt,
    SUM(passing_yards) as total_yards,
    AVG(CASE WHEN complete_pass = 1 THEN 100.0 ELSE 0.0 END) as completion_pct,
    AVG(epa) as avg_epa,
    -- Include pass depth mix for context
    SUM(CASE WHEN pass_length = 'short' THEN 1 ELSE 0 END) as short_attempts,
    SUM(CASE WHEN pass_length = 'deep' THEN 1 ELSE 0 END) as deep_attempts
  FROM bronze.nfl_play_by_play
  WHERE pass_attempt = 1 
    AND passer_player_id IS NOT NULL
    AND season >= 2021  -- Use all available data
  GROUP BY passer_player_id, passer_player_name, season, week
  HAVING COUNT(*) >= 10  -- Minimum 10 attempts per week
),

season_consistency AS (
  SELECT
    passer_player_id,
    passer_player_name,
    season,
    COUNT(DISTINCT week) as weeks_played,
    
    -- Central tendency
    AVG(avg_yards_per_attempt) as season_ypa,
    AVG(total_yards) as avg_weekly_yards,
    AVG(completion_pct) as avg_completion_pct,
    
    -- Variability measures
    STDDEV(avg_yards_per_attempt) as ypa_stddev,
    STDDEV(total_yards) as yards_stddev,
    STDDEV(completion_pct) as comp_pct_stddev,
    
    -- Coefficient of Variation (normalized variability)
    STDDEV(avg_yards_per_attempt) / NULLIF(AVG(avg_yards_per_attempt), 0) as ypa_cv,
    STDDEV(total_yards) / NULLIF(AVG(total_yards), 0) as yards_cv,
    STDDEV(completion_pct) / NULLIF(AVG(completion_pct), 0) as comp_cv,
    
    -- Volume
    SUM(attempts) as total_attempts,
    AVG(attempts) as avg_attempts_per_week
    
  FROM weekly_performance
  GROUP BY passer_player_id, passer_player_name, season
  HAVING COUNT(DISTINCT week) >= 8  -- Minimum 8 weeks for reliable variance
),

-- Calculate z-scores and percentiles
with_rankings AS (
  SELECT
    *,
    -- Z-scores (distance from mean in standard deviations)
    (season_ypa - AVG(season_ypa) OVER (PARTITION BY season)) / 
      NULLIF(STDDEV(season_ypa) OVER (PARTITION BY season), 0) as ypa_zscore,
    
    -- Percentile ranks (0-100, higher = better)
    PERCENT_RANK() OVER (PARTITION BY season ORDER BY season_ypa) * 100 as ypa_percentile,
    PERCENT_RANK() OVER (PARTITION BY season ORDER BY ypa_cv DESC) * 100 as consistency_percentile,
    
    -- Rank within season
    RANK() OVER (PARTITION BY season ORDER BY ypa_cv) as consistency_rank,
    RANK() OVER (PARTITION BY season ORDER BY season_ypa DESC) as performance_rank
    
  FROM season_consistency
),

-- Multi-year aggregation for true stability
multi_year AS (
  SELECT
    passer_player_id,
    passer_player_name,
    
    -- Aggregate across all seasons
    COUNT(DISTINCT season) as seasons_played,
    AVG(season_ypa) as career_avg_ypa,
    AVG(ypa_cv) as career_avg_cv,
    STDDEV(season_ypa) as year_to_year_stddev,
    
    -- Trend analysis
    REGR_SLOPE(season_ypa, season) as ypa_trend,
    REGR_R2(season_ypa, season) as ypa_trend_r2,
    
    -- Recent form (2024 if available)
    MAX(CASE WHEN season = 2024 THEN season_ypa END) as ypa_2024,
    MAX(CASE WHEN season = 2024 THEN ypa_cv END) as cv_2024,
    MAX(CASE WHEN season = 2024 THEN consistency_percentile END) as consistency_pct_2024,
    MAX(CASE WHEN season = 2024 THEN total_attempts END) as attempts_2024
    
  FROM with_rankings
  GROUP BY passer_player_id, passer_player_name
  HAVING COUNT(DISTINCT season) >= 2  -- Need multiple seasons for stability
)

SELECT
  passer_player_name as quarterback,
  seasons_played,
  ROUND(career_avg_ypa, 2) as avg_ypa,
  ROUND(career_avg_cv, 3) as avg_cv,
  ROUND(year_to_year_stddev, 2) as year_to_year_std,
  ROUND(ypa_trend, 3) as ypa_trend_slope,
  ROUND(ypa_trend_r2, 3) as trend_r_squared,
  ROUND(ypa_2024, 2) as current_ypa,
  ROUND(cv_2024, 3) as current_cv,
  ROUND(consistency_pct_2024, 1) as consistency_percentile,
  attempts_2024 as current_attempts,
  
  -- Classification
  CASE
    WHEN career_avg_cv < 0.25 AND career_avg_ypa > 8.5 THEN 'ELITE STABLE'
    WHEN career_avg_cv < 0.30 AND career_avg_ypa > 8.0 THEN 'HIGHLY STABLE'
    WHEN career_avg_cv < 0.35 THEN 'STABLE'
    WHEN career_avg_cv < 0.40 THEN 'MODERATE'
    ELSE 'VOLATILE'
  END as stability_class,
  
  -- Trend interpretation
  CASE
    WHEN ypa_trend > 0.2 AND ypa_trend_r2 > 0.5 THEN 'IMPROVING'
    WHEN ypa_trend < -0.2 AND ypa_trend_r2 > 0.5 THEN 'DECLINING'
    ELSE 'STEADY'
  END as trend_direction

FROM multi_year
WHERE attempts_2024 >= 200  -- Active QBs with meaningful volume
ORDER BY career_avg_cv, career_avg_ypa DESC;