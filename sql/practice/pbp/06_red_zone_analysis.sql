-- ============================================
-- Real Analysis: Red Zone Efficiency Deep Dive
-- ============================================
-- Purpose: Comprehensive red zone analysis for fantasy insights
-- Concepts: Multiple CTEs, complex joins, conditional aggregation
-- ============================================

-- Query 1: Complete red zone leaderboard (all positions)
-- Fantasy gold - who gets the opportunities near the goal line?
WITH red_zone_touches AS (
    SELECT 
        COALESCE(rusher_player_name, receiver_player_name, passer_player_name) as player_name,
        CASE 
            WHEN rusher_player_name IS NOT NULL THEN 'RB'
            WHEN receiver_player_name IS NOT NULL THEN 'WR/TE'
            WHEN passer_player_name IS NOT NULL AND qb_scramble = true THEN 'QB'
            ELSE 'Unknown'
        END as position_group,
        COUNT(*) as rz_touches,
        SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) as rz_tds,
        AVG(yards_gained) as avg_yards,
        SUM(CASE WHEN yardline_100 <= 5 THEN 1 ELSE 0 END) as goal_line_touches,
        SUM(CASE WHEN yardline_100 <= 5 AND touchdown = true THEN 1 ELSE 0 END) as goal_line_tds
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
      AND season = 2023
      AND play_type IN ('pass', 'run')
      AND (rusher_player_name IS NOT NULL 
           OR receiver_player_name IS NOT NULL 
           OR (passer_player_name IS NOT NULL AND qb_scramble = true))
    GROUP BY player_name, position_group
)
SELECT 
    player_name,
    position_group,
    rz_touches,
    rz_tds,
    ROUND(100.0 * rz_tds / rz_touches, 1) as rz_td_rate,
    goal_line_touches,
    goal_line_tds,
    CASE 
        WHEN goal_line_touches > 0 
        THEN ROUND(100.0 * goal_line_tds / goal_line_touches, 1)
        ELSE 0
    END as goal_line_td_rate,
    ROUND(avg_yards, 1) as avg_yards
FROM red_zone_touches
WHERE rz_touches >= 10
ORDER BY rz_tds DESC, rz_touches DESC
LIMIT 30;


-- Query 2: Team red zone play calling tendencies
-- Understand how teams operate in the red zone
SELECT 
    posteam,
    COUNT(*) as total_plays,
    SUM(CASE WHEN play_type = 'pass' THEN 1 ELSE 0 END) as pass_plays,
    SUM(CASE WHEN play_type = 'run' THEN 1 ELSE 0 END) as run_plays,
    ROUND(100.0 * SUM(CASE WHEN play_type = 'pass' THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_rate,
    SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) as total_tds,
    SUM(CASE WHEN pass_touchdown = true THEN 1 ELSE 0 END) as pass_tds,
    SUM(CASE WHEN rush_touchdown = true THEN 1 ELSE 0 END) as rush_tds,
    ROUND(AVG(epa), 3) as avg_epa,
    ROUND(AVG(CASE WHEN play_type = 'pass' THEN epa ELSE NULL END), 3) as pass_epa,
    ROUND(AVG(CASE WHEN play_type = 'run' THEN epa ELSE NULL END), 3) as run_epa
FROM bronze.nfl_play_by_play
WHERE yardline_100 <= 20
  AND play_type IN ('pass', 'run')
  AND season = 2023
  AND posteam IS NOT NULL
GROUP BY posteam
ORDER BY avg_epa DESC;


-- Query 3: Red zone efficiency by down and distance
-- When do teams score and when do they stall?
SELECT 
    down,
    CASE 
        WHEN ydstogo <= 3 THEN 'Short (1-3)'
        WHEN ydstogo <= 7 THEN 'Medium (4-7)'
        ELSE 'Long (8+)'
    END as distance_bucket,
    COUNT(*) as plays,
    SUM(CASE WHEN play_type = 'pass' THEN 1 ELSE 0 END) as pass_plays,
    SUM(CASE WHEN play_type = 'run' THEN 1 ELSE 0 END) as run_plays,
    SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) as touchdowns,
    ROUND(100.0 * SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) / COUNT(*), 1) as td_rate,
    ROUND(100.0 * SUM(CASE WHEN first_down = true OR touchdown = true THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate,
    ROUND(AVG(yards_gained), 1) as avg_yards,
    ROUND(AVG(epa), 3) as avg_epa
FROM bronze.nfl_play_by_play
WHERE yardline_100 <= 20
  AND down IN (1, 2, 3, 4)
  AND play_type IN ('pass', 'run')
  AND season = 2023
GROUP BY down, distance_bucket
ORDER BY down, 
         CASE 
            WHEN ydstogo <= 3 THEN 1
            WHEN ydstogo <= 7 THEN 2
            ELSE 3
         END;


-- Query 4: Individual player red zone target/carry share
-- Who dominates their team's red zone opportunities?
WITH team_rz_totals AS (
    SELECT 
        posteam,
        COUNT(CASE WHEN pass_attempt = true THEN 1 END) as team_rz_targets,
        COUNT(CASE WHEN rush_attempt = true THEN 1 END) as team_rz_carries
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
      AND season = 2023
      AND play_type IN ('pass', 'run')
    GROUP BY posteam
),
player_rz_stats AS (
    SELECT 
        COALESCE(receiver_player_name, rusher_player_name) as player_name,
        posteam,
        COUNT(CASE WHEN receiver_player_name IS NOT NULL THEN 1 END) as player_targets,
        COUNT(CASE WHEN rusher_player_name IS NOT NULL THEN 1 END) as player_carries,
        SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) as touchdowns
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
      AND season = 2023
      AND play_type IN ('pass', 'run')
      AND (receiver_player_name IS NOT NULL OR rusher_player_name IS NOT NULL)
    GROUP BY player_name, posteam
)
SELECT 
    p.player_name,
    p.posteam,
    p.player_targets,
    p.player_carries,
    p.player_targets + p.player_carries as total_opportunities,
    CASE 
        WHEN t.team_rz_targets > 0 
        THEN ROUND(100.0 * p.player_targets / t.team_rz_targets, 1)
        ELSE 0
    END as target_share_pct,
    CASE 
        WHEN t.team_rz_carries > 0 
        THEN ROUND(100.0 * p.player_carries / t.team_rz_carries, 1)
        ELSE 0
    END as carry_share_pct,
    p.touchdowns
FROM player_rz_stats p
JOIN team_rz_totals t ON p.posteam = t.posteam
WHERE p.player_targets + p.player_carries >= 5
ORDER BY total_opportunities DESC
LIMIT 40;


-- Query 5: Red zone play success by formation
-- Does formation matter in the red zone?
SELECT 
    CASE 
        WHEN shotgun = true THEN 'Shotgun'
        ELSE 'Under Center'
    END as formation,
    play_type,
    COUNT(*) as plays,
    SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) as touchdowns,
    ROUND(100.0 * SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) / COUNT(*), 1) as td_rate,
    ROUND(AVG(yards_gained), 2) as avg_yards,
    ROUND(AVG(epa), 3) as avg_epa,
    SUM(CASE WHEN yards_gained <= -3 THEN 1 ELSE 0 END) as negative_plays,
    ROUND(100.0 * SUM(CASE WHEN yards_gained <= -3 THEN 1 ELSE 0 END) / COUNT(*), 1) as negative_play_rate
FROM bronze.nfl_play_by_play
WHERE yardline_100 <= 20
  AND play_type IN ('pass', 'run')
  AND season = 2023
  AND shotgun IS NOT NULL
GROUP BY formation, play_type
ORDER BY formation, play_type;


-- PRACTICE CHALLENGES:
-- 1. Find which players have the best red zone TD rate on 1st down vs other downs
-- 2. Analyze red zone performance based on score differential (trailing/leading/tied)
-- 3. Compare red zone efficiency in different weather conditions
-- 4. Find players who excel in goal-to-go situations specifically
-- 5. Analyze how red zone efficiency changes throughout the season (early vs late)