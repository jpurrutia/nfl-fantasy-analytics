-- Mean Reversion Analysis for Fantasy Football
-- Statistical Principle: Extreme performances tend to regress toward the mean
-- Based on regression to the mean coefficient: r = 1 - R²

WITH historical_performance AS (
  SELECT
    passer_player_id,
    passer_player_name,
    season,
    pass_length,
    COUNT(*) as attempts,
    AVG(passing_yards) as ypa,
    AVG(CASE WHEN complete_pass = 1 THEN 100.0 ELSE 0.0 END) as comp_pct,
    AVG(epa) as epa_per_attempt,
    -- Calculate TD rate and INT rate
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as td_rate,
    SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as int_rate
  FROM bronze.nfl_play_by_play
  WHERE pass_attempt = 1
    AND passer_player_id IS NOT NULL
    AND season >= 2021
  GROUP BY passer_player_id, passer_player_name, season, pass_length
),

league_baselines AS (
  SELECT
    season,
    pass_length,
    AVG(ypa) as league_avg_ypa,
    STDDEV(ypa) as league_std_ypa,
    AVG(comp_pct) as league_avg_comp,
    STDDEV(comp_pct) as league_std_comp,
    AVG(td_rate) as league_avg_td_rate,
    STDDEV(td_rate) as league_std_td_rate,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ypa) as ypa_q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY ypa) as ypa_median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ypa) as ypa_q3,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ypa) as ypa_p90,
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY ypa) as ypa_p10
  FROM historical_performance
  WHERE attempts >= 30  -- Minimum sample size
  GROUP BY season, pass_length
),

player_with_zscore AS (
  SELECT
    h.*,
    l.league_avg_ypa,
    l.league_std_ypa,
    l.ypa_median,
    l.ypa_p90,
    l.ypa_p10,
    -- Calculate z-scores (distance from mean in standard deviations)
    (h.ypa - l.league_avg_ypa) / NULLIF(l.league_std_ypa, 0) as ypa_zscore,
    (h.comp_pct - l.league_avg_comp) / NULLIF(l.league_std_comp, 0) as comp_zscore,
    (h.td_rate - l.league_avg_td_rate) / NULLIF(l.league_std_td_rate, 0) as td_rate_zscore
  FROM historical_performance h
  JOIN league_baselines l 
    ON h.season = l.season 
    AND h.pass_length = l.pass_length
),

year_over_year AS (
  SELECT
    passer_player_id,
    passer_player_name,
    season,
    pass_length,
    attempts,
    ypa,
    ypa_zscore,
    comp_pct,
    td_rate,
    td_rate_zscore,
    -- Previous year metrics
    LAG(ypa) OVER (PARTITION BY passer_player_id, pass_length ORDER BY season) as prev_ypa,
    LAG(ypa_zscore) OVER (PARTITION BY passer_player_id, pass_length ORDER BY season) as prev_zscore,
    LAG(td_rate) OVER (PARTITION BY passer_player_id, pass_length ORDER BY season) as prev_td_rate,
    LAG(attempts) OVER (PARTITION BY passer_player_id, pass_length ORDER BY season) as prev_attempts,
    -- League context
    league_avg_ypa,
    ypa_median,
    ypa_p90,
    ypa_p10
  FROM player_with_zscore
),

regression_analysis AS (
  SELECT
    passer_player_id,
    passer_player_name,
    pass_length,
    season,
    attempts,
    ROUND(ypa, 2) as current_ypa,
    ROUND(prev_ypa, 2) as previous_ypa,
    ROUND(ypa_zscore, 2) as current_zscore,
    ROUND(prev_zscore, 2) as previous_zscore,
    ROUND(td_rate, 2) as td_rate_pct,
    ROUND(league_avg_ypa, 2) as league_avg,
    
    -- Identify outliers (|z-score| > 2)
    CASE 
      WHEN ABS(ypa_zscore) > 2 THEN 'EXTREME OUTLIER'
      WHEN ABS(ypa_zscore) > 1.5 THEN 'OUTLIER'
      WHEN ABS(ypa_zscore) > 1 THEN 'ABOVE/BELOW AVG'
      ELSE 'NORMAL'
    END as performance_category,
    
    -- Regression expectation based on current z-score
    -- Using regression coefficient of ~0.6 for QB stats
    ROUND(
      league_avg_ypa + (ypa - league_avg_ypa) * 0.4,  -- Keep 40% of deviation
      2
    ) as expected_ypa_next,
    
    -- Confidence in regression (based on how extreme the outlier is)
    CASE
      WHEN ypa_zscore > 2.5 THEN 'HIGH REGRESSION RISK'
      WHEN ypa_zscore > 1.5 THEN 'MODERATE REGRESSION RISK'
      WHEN ypa_zscore < -2.5 THEN 'HIGH POSITIVE REGRESSION'
      WHEN ypa_zscore < -1.5 THEN 'MODERATE POSITIVE REGRESSION'
      ELSE 'STABLE'
    END as regression_outlook,
    
    -- Historical regression accuracy (if we have prev year data)
    CASE
      WHEN prev_zscore IS NOT NULL THEN
        ROUND(ABS(ypa_zscore - prev_zscore * 0.4), 2)  -- How well did regression predict?
      ELSE NULL
    END as regression_error
    
  FROM year_over_year
  WHERE season = 2024
    AND attempts >= 50  -- Minimum sample for current season
),

final_summary AS (
  SELECT
    passer_player_name as quarterback,
    -- Aggregate short and deep pass analysis
    MAX(CASE WHEN pass_length = 'short' THEN current_ypa END) as short_ypa,
    MAX(CASE WHEN pass_length = 'deep' THEN current_ypa END) as deep_ypa,
    MAX(CASE WHEN pass_length = 'short' THEN current_zscore END) as short_zscore,
    MAX(CASE WHEN pass_length = 'deep' THEN current_zscore END) as deep_zscore,
    MAX(CASE WHEN pass_length = 'short' THEN expected_ypa_next END) as exp_short_ypa,
    MAX(CASE WHEN pass_length = 'deep' THEN expected_ypa_next END) as exp_deep_ypa,
    MAX(CASE WHEN pass_length = 'short' THEN td_rate_pct END) as short_td_rate,
    MAX(CASE WHEN pass_length = 'deep' THEN td_rate_pct END) as deep_td_rate,
    
    -- Overall regression risk
    CASE
      WHEN MAX(CASE WHEN pass_length = 'deep' THEN current_zscore END) > 2 
       AND MAX(CASE WHEN pass_length = 'short' THEN current_zscore END) < 1 
      THEN 'SELL HIGH - Deep unsustainable'
      
      WHEN MAX(CASE WHEN pass_length = 'short' THEN current_zscore END) > 2
      THEN 'SELL HIGH - Overall unsustainable'
      
      WHEN MAX(CASE WHEN pass_length = 'short' THEN current_zscore END) < -1.5
       AND MAX(CASE WHEN pass_length = 'deep' THEN current_zscore END) < -1
      THEN 'BUY LOW - Due for positive regression'
      
      WHEN MAX(CASE WHEN pass_length = 'deep' THEN td_rate_pct END) > 7
      THEN 'TD REGRESSION LIKELY'
      
      ELSE 'HOLD - Performance sustainable'
    END as fantasy_recommendation,
    
    -- Total attempts for context
    SUM(attempts) as total_attempts
    
  FROM regression_analysis
  GROUP BY passer_player_name
  HAVING SUM(attempts) >= 200  -- Minimum total attempts
)

SELECT 
  quarterback,
  short_ypa,
  deep_ypa,
  short_zscore as short_z,
  deep_zscore as deep_z,
  exp_short_ypa as exp_short,
  exp_deep_ypa as exp_deep,
  ROUND(short_td_rate, 1) as short_td_pct,
  ROUND(deep_td_rate, 1) as deep_td_pct,
  fantasy_recommendation,
  total_attempts
FROM final_summary
ORDER BY 
  CASE 
    WHEN fantasy_recommendation LIKE 'SELL HIGH%' THEN 1
    WHEN fantasy_recommendation LIKE 'BUY LOW%' THEN 2
    WHEN fantasy_recommendation = 'TD REGRESSION LIKELY' THEN 3
    ELSE 4
  END,
  ABS(deep_zscore) DESC;