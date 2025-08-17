-- ============================================
-- Real Analysis: WR/TE Target Distribution & Efficiency
-- ============================================
-- Purpose: Deep dive into receiver usage patterns for fantasy
-- Concepts: Target share, air yards, YAC, catchable targets
-- ============================================

-- Query 1: Complete receiver metrics with target quality
-- Everything you need to evaluate a receiver
WITH receiver_stats AS (
    SELECT 
        receiver_player_name,
        posteam,
        COUNT(*) as targets,
        SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as receptions,
        SUM(receiving_yards) as total_yards,
        SUM(yards_after_catch) as total_yac,
        SUM(air_yards) as total_air_yards,
        SUM(pass_touchdown) as touchdowns,
        AVG(air_yards) as avg_air_yards,
        AVG(CASE WHEN complete_pass = true THEN yards_after_catch ELSE NULL END) as avg_yac,
        AVG(epa) as avg_epa,
        SUM(CASE WHEN air_yards >= 20 THEN 1 ELSE 0 END) as deep_targets,
        SUM(CASE WHEN air_yards >= 20 AND complete_pass = true THEN 1 ELSE 0 END) as deep_receptions
    FROM bronze.nfl_play_by_play
    WHERE receiver_player_name IS NOT NULL
      AND pass_attempt = true
      AND season = 2023
    GROUP BY receiver_player_name, posteam
)
SELECT 
    receiver_player_name,
    posteam,
    targets,
    receptions,
    ROUND(100.0 * receptions / targets, 1) as catch_rate,
    total_yards,
    ROUND(total_yards * 1.0 / receptions, 1) as yards_per_reception,
    ROUND(total_yards * 1.0 / targets, 1) as yards_per_target,
    touchdowns,
    ROUND(avg_air_yards, 1) as avg_air_yards,
    ROUND(avg_yac, 1) as avg_yac,
    ROUND(100.0 * total_yac / NULLIF(total_yards, 0), 1) as yac_percentage,
    deep_targets,
    CASE 
        WHEN deep_targets > 0 
        THEN ROUND(100.0 * deep_receptions / deep_targets, 1)
        ELSE 0
    END as deep_catch_rate,
    ROUND(avg_epa, 3) as avg_epa
FROM receiver_stats
WHERE targets >= 30
ORDER BY total_yards DESC
LIMIT 40;


-- Query 2: Target share and team involvement
-- Who dominates their team's passing game?
WITH team_totals AS (
    SELECT 
        posteam,
        COUNT(*) as team_targets,
        SUM(passing_yards) as team_passing_yards,
        SUM(air_yards) as team_air_yards
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true
      AND season = 2023
      AND posteam IS NOT NULL
    GROUP BY posteam
),
player_totals AS (
    SELECT 
        receiver_player_name,
        posteam,
        COUNT(*) as player_targets,
        SUM(receiving_yards) as player_yards,
        SUM(air_yards) as player_air_yards,
        SUM(pass_touchdown) as player_tds
    FROM bronze.nfl_play_by_play
    WHERE receiver_player_name IS NOT NULL
      AND pass_attempt = true
      AND season = 2023
    GROUP BY receiver_player_name, posteam
)
SELECT 
    p.receiver_player_name,
    p.posteam,
    p.player_targets,
    ROUND(100.0 * p.player_targets / t.team_targets, 1) as target_share,
    p.player_yards,
    ROUND(100.0 * p.player_yards / t.team_passing_yards, 1) as yards_share,
    ROUND(100.0 * p.player_air_yards / NULLIF(t.team_air_yards, 0), 1) as air_yards_share,
    p.player_tds,
    -- Weighted opportunity score (targets + air yards share)
    ROUND((100.0 * p.player_targets / t.team_targets) + 
          (100.0 * p.player_air_yards / NULLIF(t.team_air_yards, 0) * 0.5), 1) as opportunity_score
FROM player_totals p
JOIN team_totals t ON p.posteam = t.posteam
WHERE p.player_targets >= 30
ORDER BY opportunity_score DESC
LIMIT 30;


-- Query 3: Receiver performance by route depth
-- Who excels at different levels of the field?
WITH route_depth_stats AS (
    SELECT 
        receiver_player_name,
        CASE 
            WHEN air_yards < 0 THEN 'Behind LOS'
            WHEN air_yards <= 5 THEN 'Short (0-5)'
            WHEN air_yards <= 15 THEN 'Intermediate (6-15)'
            ELSE 'Deep (16+)'
        END as route_depth,
        COUNT(*) as targets,
        SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as catches,
        SUM(receiving_yards) as yards,
        SUM(yards_after_catch) as yac,
        SUM(pass_touchdown) as touchdowns,
        AVG(epa) as avg_epa
    FROM bronze.nfl_play_by_play
    WHERE receiver_player_name IS NOT NULL
      AND pass_attempt = true
      AND season = 2023
      AND air_yards IS NOT NULL
    GROUP BY receiver_player_name, route_depth
)
SELECT 
    receiver_player_name,
    route_depth,
    targets,
    catches,
    ROUND(100.0 * catches / targets, 1) as catch_rate,
    yards,
    ROUND(yards * 1.0 / NULLIF(catches, 0), 1) as yards_per_catch,
    ROUND(100.0 * yac / NULLIF(yards, 0), 1) as yac_pct,
    touchdowns,
    ROUND(avg_epa, 3) as avg_epa
FROM route_depth_stats
WHERE receiver_player_name IN (
    SELECT receiver_player_name
    FROM bronze.nfl_play_by_play
    WHERE receiver_player_name IS NOT NULL
      AND pass_attempt = true
      AND season = 2023
    GROUP BY receiver_player_name
    HAVING COUNT(*) >= 50
)
ORDER BY receiver_player_name, 
         CASE route_depth
            WHEN 'Behind LOS' THEN 1
            WHEN 'Short (0-5)' THEN 2
            WHEN 'Intermediate (6-15)' THEN 3
            ELSE 4
         END;


-- Query 4: Slot vs outside receiver analysis
-- Using pass_location to infer alignment
SELECT 
    receiver_player_name,
    pass_location,
    COUNT(*) as targets,
    SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as catches,
    ROUND(100.0 * SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) / COUNT(*), 1) as catch_rate,
    SUM(receiving_yards) as yards,
    AVG(air_yards) as avg_air_yards,
    AVG(yards_after_catch) as avg_yac,
    SUM(pass_touchdown) as touchdowns,
    ROUND(AVG(epa), 3) as avg_epa
FROM bronze.nfl_play_by_play
WHERE receiver_player_name IS NOT NULL
  AND pass_attempt = true
  AND pass_location IN ('left', 'middle', 'right')
  AND season = 2023
GROUP BY receiver_player_name, pass_location
HAVING COUNT(*) >= 15
ORDER BY receiver_player_name, pass_location;


-- Query 5: Receiver consistency and boom/bust potential
-- Key for fantasy - who gives you steady production vs big games?
WITH weekly_stats AS (
    SELECT 
        week,
        receiver_player_name,
        COUNT(*) as targets,
        SUM(receiving_yards) as yards,
        SUM(pass_touchdown) as tds,
        SUM(receiving_yards) + (SUM(pass_touchdown) * 6) as fantasy_points
    FROM bronze.nfl_play_by_play
    WHERE receiver_player_name IS NOT NULL
      AND pass_attempt = true
      AND season = 2023
    GROUP BY week, receiver_player_name
),
consistency_stats AS (
    SELECT 
        receiver_player_name,
        COUNT(DISTINCT week) as games_targeted,
        AVG(fantasy_points) as avg_fantasy_points,
        STDDEV(fantasy_points) as stddev_fantasy_points,
        MAX(fantasy_points) as best_game,
        MIN(fantasy_points) as worst_game,
        SUM(CASE WHEN fantasy_points >= 15 THEN 1 ELSE 0 END) as boom_games,
        SUM(CASE WHEN fantasy_points < 8 THEN 1 ELSE 0 END) as bust_games
    FROM weekly_stats
    GROUP BY receiver_player_name
    HAVING COUNT(DISTINCT week) >= 10
)
SELECT 
    receiver_player_name,
    games_targeted,
    ROUND(avg_fantasy_points, 1) as avg_fantasy_points,
    ROUND(stddev_fantasy_points, 1) as consistency_score,
    ROUND(avg_fantasy_points / NULLIF(stddev_fantasy_points, 0), 2) as stability_ratio,
    best_game,
    worst_game,
    boom_games,
    ROUND(100.0 * boom_games / games_targeted, 1) as boom_rate,
    bust_games,
    ROUND(100.0 * bust_games / games_targeted, 1) as bust_rate
FROM consistency_stats
ORDER BY avg_fantasy_points DESC
LIMIT 30;


-- PRACTICE CHALLENGES:
-- 1. Find receivers with the best catch rate when targeted 10+ yards downfield
-- 2. Analyze receiver performance against zone vs man coverage (using defensive metrics)
-- 3. Find which receivers see the biggest target increase in garbage time
-- 4. Compare 1st half vs 2nd half target distribution changes
-- 5. Identify receivers who excel on 3rd downs vs early downs