-- ============================================
-- SQL Practice: Aggregations with Play-by-Play Data
-- ============================================
-- Purpose: Learn GROUP BY, COUNT, SUM, AVG with NFL data
-- Concepts: GROUP BY, aggregate functions, HAVING
-- ============================================

-- Query 1: Count plays by type
-- Try modifying: Add WHERE clause for specific team or week
SELECT 
    play_type,
    COUNT(*) as play_count,
    AVG(yards_gained) as avg_yards,
    MAX(yards_gained) as max_gain
FROM bronze.nfl_play_by_play
WHERE play_type IS NOT NULL
  AND season = 2023
GROUP BY play_type
ORDER BY play_count DESC;


-- Query 2: QB performance summary
-- Try modifying: Filter for minimum attempts, or specific weeks
SELECT 
    passer_player_name,
    COUNT(*) as attempts,
    SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as completions,
    ROUND(100.0 * SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) / COUNT(*), 1) as completion_pct,
    SUM(passing_yards) as total_yards,
    SUM(pass_touchdown) as touchdowns,
    SUM(interception) as interceptions
FROM bronze.nfl_play_by_play
WHERE pass_attempt = true
  AND season = 2023
GROUP BY passer_player_name
HAVING COUNT(*) >= 100  -- Minimum 100 attempts
ORDER BY total_yards DESC
LIMIT 20;


-- Query 3: Team red zone efficiency
-- Try modifying: Look at goal line (yardline_100 <= 5) or by week
SELECT 
    posteam,
    COUNT(*) as red_zone_plays,
    SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) as touchdowns,
    ROUND(100.0 * SUM(CASE WHEN touchdown = true THEN 1 ELSE 0 END) / COUNT(*), 1) as td_rate
FROM bronze.nfl_play_by_play
WHERE yardline_100 <= 20
  AND play_type IN ('pass', 'run')
  AND season = 2023
  AND posteam IS NOT NULL
GROUP BY posteam
HAVING COUNT(*) >= 20  -- Minimum 20 red zone plays
ORDER BY td_rate DESC;


-- Query 4: Running back usage and efficiency
-- Try modifying: Add receiving stats, or filter by team
SELECT 
    rusher_player_name,
    COUNT(*) as carries,
    SUM(rushing_yards) as total_yards,
    ROUND(AVG(rushing_yards), 2) as yards_per_carry,
    MAX(rushing_yards) as longest_run,
    SUM(rush_touchdown) as touchdowns
FROM bronze.nfl_play_by_play
WHERE rush_attempt = true
  AND season = 2023
  AND rusher_player_name IS NOT NULL
GROUP BY rusher_player_name
HAVING COUNT(*) >= 50  -- Minimum 50 carries
ORDER BY total_yards DESC
LIMIT 25;


-- Query 5: Wide receiver target distribution
-- Try modifying: Add yards after catch, or filter by team
SELECT 
    receiver_player_name,
    COUNT(*) as targets,
    SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as receptions,
    ROUND(100.0 * SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) / COUNT(*), 1) as catch_rate,
    SUM(receiving_yards) as receiving_yards,
    SUM(pass_touchdown) as touchdowns,
    ROUND(AVG(air_yards), 1) as avg_air_yards
FROM bronze.nfl_play_by_play
WHERE receiver_player_name IS NOT NULL
  AND pass_attempt = true
  AND season = 2023
GROUP BY receiver_player_name
HAVING COUNT(*) >= 30  -- Minimum 30 targets
ORDER BY receiving_yards DESC
LIMIT 25;


-- Query 6: Team performance by down
-- Try modifying: Look at specific distance ranges (ydstogo buckets)
SELECT 
    posteam,
    down,
    COUNT(*) as plays,
    ROUND(AVG(yards_gained), 2) as avg_yards,
    SUM(CASE WHEN first_down = true THEN 1 ELSE 0 END) as first_downs,
    ROUND(100.0 * SUM(CASE WHEN first_down = true THEN 1 ELSE 0 END) / COUNT(*), 1) as conversion_rate
FROM bronze.nfl_play_by_play
WHERE down IN (1, 2, 3, 4)
  AND play_type IN ('pass', 'run')
  AND season = 2023
  AND posteam IS NOT NULL
GROUP BY posteam, down
ORDER BY posteam, down;


-- PRACTICE CHALLENGES:
-- 1. Calculate each team's average EPA per play
-- 2. Find QBs with the best completion % on deep passes (air_yards > 20)
-- 3. Calculate fumble rate by player (fumbles per touch)
-- 4. Find teams with the best 3rd down conversion rate by distance buckets
-- 5. Calculate average starting field position by team