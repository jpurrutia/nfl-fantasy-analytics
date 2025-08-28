-- Enhanced QB Stability Analysis with Completion % and Stability Scores
-- Based on concept: short passes are 2x more stable than deep passes

WITH bounds AS (
  SELECT MAX(season) AS max_season
  FROM bronze.nfl_play_by_play
),

pass_metrics AS (
  SELECT
    passer_player_id AS passer_id,
    passer_player_name AS passer_name,
    season,
    pass_length,
    COUNT(1) AS attempts,
    AVG(passing_yards) AS ypa,
    AVG(CASE WHEN complete_pass = 1 THEN 100.0 ELSE 0.0 END) AS completion_pct,
    AVG(epa) AS epa_per_attempt,
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS touchdowns,
    SUM(CASE WHEN interception = 1 THEN 1 ELSE 0 END) AS interceptions
  FROM bronze.nfl_play_by_play
  JOIN bounds b ON TRUE
  WHERE season IN (b.max_season, b.max_season - 1)
    AND pass_attempt = 1
    AND pass_length IN ('deep', 'short')
    AND passer_player_id IS NOT NULL
  GROUP BY passer_id, passer_name, season, pass_length
),

with_prev AS (
  SELECT 
    passer_id,
    passer_name,
    pass_length,
    season,
    attempts,
    ypa,
    completion_pct,
    epa_per_attempt,
    touchdowns,
    interceptions,
    -- Previous year metrics
    LAG(ypa) OVER (PARTITION BY passer_id, pass_length ORDER BY season) AS ypa_prev,
    LAG(completion_pct) OVER (PARTITION BY passer_id, pass_length ORDER BY season) AS comp_pct_prev,
    LAG(epa_per_attempt) OVER (PARTITION BY passer_id, pass_length ORDER BY season) AS epa_prev
  FROM pass_metrics
),

current_season AS (
  SELECT *
  FROM with_prev
  WHERE season = (SELECT max_season FROM bounds)
),

-- Calculate stability (year-over-year change)
stability_scores AS (
  SELECT
    passer_id,
    passer_name,
    pass_length,
    season,
    attempts,
    ROUND(ypa, 2) AS ypa,
    ROUND(ypa_prev, 2) AS ypa_prev,
    ROUND(completion_pct, 1) AS comp_pct,
    ROUND(comp_pct_prev, 1) AS comp_pct_prev,
    ROUND(epa_per_attempt, 3) AS epa,
    ROUND(epa_prev, 3) AS epa_prev,
    touchdowns,
    interceptions,
    -- Calculate stability metrics (smaller change = more stable)
    ROUND(ABS(ypa - ypa_prev), 2) AS ypa_change,
    ROUND(ABS(completion_pct - comp_pct_prev), 1) AS comp_pct_change,
    -- Stability score: inverse of normalized change (0-100, higher = more stable)
    CASE 
      WHEN ypa_prev IS NOT NULL THEN
        ROUND(100 - (ABS(ypa - ypa_prev) / NULLIF(ypa_prev, 0) * 100), 1)
      ELSE NULL
    END AS ypa_stability_score
  FROM current_season
),

-- Aggregate by QB with position-specific weights
qb_summary AS (
  SELECT
    passer_id,
    passer_name,
    season,
    -- Short pass metrics (more stable, weight = 2)
    MAX(CASE WHEN pass_length = 'short' THEN attempts END) AS short_attempts,
    MAX(CASE WHEN pass_length = 'short' THEN ypa END) AS short_ypa,
    MAX(CASE WHEN pass_length = 'short' THEN ypa_prev END) AS short_ypa_prev,
    MAX(CASE WHEN pass_length = 'short' THEN comp_pct END) AS short_comp_pct,
    MAX(CASE WHEN pass_length = 'short' THEN ypa_stability_score END) AS short_stability,
    
    -- Deep pass metrics (less stable, weight = 1)
    MAX(CASE WHEN pass_length = 'deep' THEN attempts END) AS deep_attempts,
    MAX(CASE WHEN pass_length = 'deep' THEN ypa END) AS deep_ypa,
    MAX(CASE WHEN pass_length = 'deep' THEN ypa_prev END) AS deep_ypa_prev,
    MAX(CASE WHEN pass_length = 'deep' THEN comp_pct END) AS deep_comp_pct,
    MAX(CASE WHEN pass_length = 'deep' THEN ypa_stability_score END) AS deep_stability,
    
    -- Composite stability score (weighted average: short=2x, deep=1x)
    ROUND(
      (COALESCE(MAX(CASE WHEN pass_length = 'short' THEN ypa_stability_score END), 50) * 2 +
       COALESCE(MAX(CASE WHEN pass_length = 'deep' THEN ypa_stability_score END), 50) * 1) / 3,
      1
    ) AS composite_stability_score,
    
    -- Regression flag: high deep YPA without corresponding short pass efficiency
    CASE
      WHEN MAX(CASE WHEN pass_length = 'deep' THEN ypa END) > 28
       AND MAX(CASE WHEN pass_length = 'short' THEN ypa END) < 8.5
       AND MAX(CASE WHEN pass_length = 'deep' THEN ypa_prev END) IS NOT NULL
      THEN 'REGRESSION CANDIDATE'
      WHEN MAX(CASE WHEN pass_length = 'short' THEN ypa_stability_score END) > 90
       AND MAX(CASE WHEN pass_length = 'deep' THEN ypa_stability_score END) > 80
      THEN 'HIGHLY STABLE'
      ELSE 'NORMAL'
    END AS stability_category
    
  FROM stability_scores
  GROUP BY passer_id, passer_name, season
)

SELECT
  passer_name AS quarterback,
  short_attempts,
  deep_attempts,
  short_ypa,
  deep_ypa,
  short_ypa_prev,
  deep_ypa_prev,
  short_comp_pct,
  deep_comp_pct,
  short_stability,
  deep_stability,
  composite_stability_score,
  stability_category,
  -- Calculate expected regression
  ROUND(
    CASE 
      WHEN deep_ypa_prev IS NOT NULL THEN
        -- Expect deep YPA to regress toward mean more than short
        deep_ypa - ((deep_ypa - 27.0) * 0.4)  -- 40% regression to mean
      ELSE deep_ypa
    END, 1
  ) AS expected_deep_ypa_next,
  ROUND(
    CASE
      WHEN short_ypa_prev IS NOT NULL THEN
        -- Short YPA regresses less
        short_ypa - ((short_ypa - 8.5) * 0.2)  -- 20% regression to mean
      ELSE short_ypa
    END, 1
  ) AS expected_short_ypa_next
FROM qb_summary
WHERE short_attempts > 100 AND deep_attempts > 30
ORDER BY composite_stability_score DESC;