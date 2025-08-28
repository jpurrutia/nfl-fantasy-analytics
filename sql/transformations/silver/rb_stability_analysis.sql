-- RB Stability Analysis: Rushing vs Receiving
-- Concept: Receiving work is more stable/predictive than rushing work for RBs

WITH bounds AS (
  SELECT MAX(season) AS max_season
  FROM bronze.nfl_play_by_play
),

rushing_metrics AS (
  SELECT
    rusher_player_id AS player_id,
    rusher_player_name AS player_name,
    season,
    COUNT(1) AS carries,
    AVG(rushing_yards) AS yards_per_carry,
    SUM(rushing_yards) AS total_rush_yards,
    AVG(epa) AS rush_epa,
    SUM(CASE WHEN rush_touchdown = 1 THEN 1 ELSE 0 END) AS rush_tds,
    -- Red zone carries
    SUM(CASE WHEN yardline_100 <= 20 THEN 1 ELSE 0 END) AS rz_carries
  FROM bronze.nfl_play_by_play
  JOIN bounds b ON TRUE
  WHERE season IN (b.max_season, b.max_season - 1)
    AND rush_attempt = 1
    AND rusher_player_id IS NOT NULL
  GROUP BY rusher_player_id, rusher_player_name, season
),

receiving_metrics AS (
  SELECT
    receiver_player_id AS player_id,
    receiver_player_name AS player_name,
    season,
    COUNT(1) AS targets,
    SUM(CASE WHEN complete_pass = 1 THEN 1 ELSE 0 END) AS receptions,
    AVG(CASE WHEN complete_pass = 1 THEN yards_after_catch ELSE 0 END) AS avg_yac,
    SUM(CASE WHEN complete_pass = 1 THEN yards_gained ELSE 0 END) AS total_rec_yards,
    AVG(epa) AS rec_epa,
    SUM(CASE WHEN pass_touchdown = 1 THEN 1 ELSE 0 END) AS rec_tds,
    -- Catch rate
    AVG(CASE WHEN complete_pass = 1 THEN 100.0 ELSE 0.0 END) AS catch_rate
  FROM bronze.nfl_play_by_play
  JOIN bounds b ON TRUE
  WHERE season IN (b.max_season, b.max_season - 1)
    AND pass_attempt = 1
    AND receiver_player_id IS NOT NULL
  GROUP BY receiver_player_id, receiver_player_name, season
),

-- Combine rushing and receiving for RBs
rb_combined AS (
  SELECT
    COALESCE(r.player_id, rc.player_id) AS player_id,
    COALESCE(r.player_name, rc.player_name) AS player_name,
    COALESCE(r.season, rc.season) AS season,
    -- Rushing metrics
    COALESCE(r.carries, 0) AS carries,
    ROUND(COALESCE(r.yards_per_carry, 0), 2) AS ypc,
    COALESCE(r.total_rush_yards, 0) AS rush_yards,
    COALESCE(r.rush_tds, 0) AS rush_tds,
    COALESCE(r.rz_carries, 0) AS rz_carries,
    ROUND(COALESCE(r.rush_epa, 0), 3) AS rush_epa,
    -- Receiving metrics  
    COALESCE(rc.targets, 0) AS targets,
    COALESCE(rc.receptions, 0) AS receptions,
    ROUND(COALESCE(rc.catch_rate, 0), 1) AS catch_rate,
    COALESCE(rc.total_rec_yards, 0) AS rec_yards,
    COALESCE(rc.rec_tds, 0) AS rec_tds,
    ROUND(COALESCE(rc.avg_yac, 0), 2) AS avg_yac,
    ROUND(COALESCE(rc.rec_epa, 0), 3) AS rec_epa,
    -- Total touches and production
    COALESCE(r.carries, 0) + COALESCE(rc.receptions, 0) AS total_touches,
    COALESCE(r.total_rush_yards, 0) + COALESCE(rc.total_rec_yards, 0) AS total_yards,
    -- Usage ratio (receiving work %)
    ROUND(
      CASE 
        WHEN (COALESCE(r.carries, 0) + COALESCE(rc.targets, 0)) > 0
        THEN COALESCE(rc.targets, 0) * 100.0 / (COALESCE(r.carries, 0) + COALESCE(rc.targets, 0))
        ELSE 0
      END, 1
    ) AS receiving_usage_pct
  FROM rushing_metrics r
  FULL OUTER JOIN receiving_metrics rc 
    ON r.player_id = rc.player_id 
    AND r.season = rc.season
),

-- Add previous year metrics for stability calculation
with_prev AS (
  SELECT
    *,
    -- Previous year metrics
    LAG(ypc) OVER (PARTITION BY player_id ORDER BY season) AS ypc_prev,
    LAG(rush_yards) OVER (PARTITION BY player_id ORDER BY season) AS rush_yards_prev,
    LAG(targets) OVER (PARTITION BY player_id ORDER BY season) AS targets_prev,
    LAG(rec_yards) OVER (PARTITION BY player_id ORDER BY season) AS rec_yards_prev,
    LAG(catch_rate) OVER (PARTITION BY player_id ORDER BY season) AS catch_rate_prev,
    LAG(receiving_usage_pct) OVER (PARTITION BY player_id ORDER BY season) AS rec_usage_prev,
    LAG(total_touches) OVER (PARTITION BY player_id ORDER BY season) AS touches_prev,
    LAG(total_yards) OVER (PARTITION BY player_id ORDER BY season) AS total_yards_prev
  FROM rb_combined
),

current_season AS (
  SELECT *
  FROM with_prev
  WHERE season = (SELECT max_season FROM bounds)
    -- Filter for RBs with meaningful volume
    AND (carries >= 50 OR targets >= 30)
),

-- Calculate stability scores
stability_analysis AS (
  SELECT
    player_name,
    season,
    carries,
    targets,
    total_touches,
    ypc,
    ypc_prev,
    catch_rate,
    catch_rate_prev,
    rush_yards,
    rec_yards,
    total_yards,
    receiving_usage_pct,
    rec_usage_prev,
    -- Stability metrics
    CASE 
      WHEN ypc_prev IS NOT NULL THEN
        ROUND(100 - ABS(ypc - ypc_prev) * 20, 1)  -- Rushing is less stable
      ELSE 50.0
    END AS rush_stability_score,
    CASE
      WHEN catch_rate_prev IS NOT NULL THEN  
        ROUND(100 - ABS(catch_rate - catch_rate_prev) * 0.5, 1)  -- Receiving is more stable
      ELSE 50.0
    END AS rec_stability_score,
    CASE
      WHEN rec_usage_prev IS NOT NULL THEN
        ROUND(100 - ABS(receiving_usage_pct - rec_usage_prev) * 1.5, 1)
      ELSE 50.0
    END AS usage_stability_score,
    -- Classify RB type
    CASE
      WHEN receiving_usage_pct >= 40 THEN 'SATELLITE BACK'
      WHEN receiving_usage_pct >= 25 THEN 'DUAL-THREAT'
      WHEN receiving_usage_pct >= 15 THEN 'TRADITIONAL'
      ELSE 'POWER BACK'
    END AS rb_type,
    -- Regression flags
    CASE
      WHEN ypc > 5.0 AND ypc_prev < 4.5 THEN 'YPC REGRESSION LIKELY'
      WHEN targets > 80 AND targets_prev < 60 THEN 'TARGET REGRESSION LIKELY'
      WHEN ypc > 4.8 AND receiving_usage_pct < 15 THEN 'TD DEPENDENT'
      ELSE 'STABLE'
    END AS outlook
  FROM current_season
)

SELECT
  player_name AS running_back,
  carries,
  ypc,
  rush_yards,
  targets,
  catch_rate,
  rec_yards,
  total_touches,
  total_yards,
  receiving_usage_pct,
  rb_type,
  ROUND(rush_stability_score, 1) AS rush_stability,
  ROUND(rec_stability_score, 1) AS rec_stability,
  ROUND(usage_stability_score, 1) AS usage_stability,
  -- Composite stability (weighted: receiving 2x, rushing 1x)
  ROUND(
    (COALESCE(rush_stability_score, 50) * 1 + 
     COALESCE(rec_stability_score, 50) * 2 +
     COALESCE(usage_stability_score, 50) * 1.5) / 4.5,
    1
  ) AS composite_stability,
  outlook,
  -- Expected next year production
  ROUND(
    CASE
      WHEN ypc_prev IS NOT NULL THEN
        -- Rushing regresses heavily to mean (4.2 YPC)
        ypc - ((ypc - 4.2) * 0.5)
      ELSE ypc
    END, 2
  ) AS expected_ypc_next,
  ROUND(
    CASE  
      WHEN catch_rate_prev IS NOT NULL THEN
        -- Catch rate regresses less
        catch_rate - ((catch_rate - 75.0) * 0.25)
      ELSE catch_rate
    END, 1
  ) AS expected_catch_rate_next
FROM stability_analysis
WHERE total_touches >= 100  -- Minimum volume threshold
ORDER BY composite_stability DESC;