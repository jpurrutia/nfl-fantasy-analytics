-- ============================================
-- SQL Practice: Basic Filtering with Play-by-Play Data
-- ============================================
-- Purpose: Learn WHERE clauses and basic filtering with NFL data
-- Concepts: WHERE, AND, OR, IN, comparison operators
-- ============================================

-- Query 1: Find all touchdown plays
-- Try modifying: Change to field goals, or two-point attempts
SELECT 
    game_id,
    posteam,
    play_desc,
    td_player_name,
    yards_gained
FROM bronze.nfl_play_by_play
WHERE touchdown = true
  AND season = 2023
LIMIT 20;


-- Query 2: Find all plays by a specific player (Patrick Mahomes)
-- Try modifying: Change to your favorite QB or use rusher_player_name for RBs
SELECT 
    week,
    posteam,
    defteam,
    play_type,
    yards_gained,
    passing_yards,
    play_desc
FROM bronze.nfl_play_by_play  
WHERE passer_player_name = 'P.Mahomes'
  AND season = 2023
ORDER BY week, play_id
LIMIT 25;


-- Query 3: Find all red zone plays (within 20 yards of end zone)
-- Try modifying: Change to goal line (yardline_100 <= 5) or midfield plays
SELECT 
    game_id,
    posteam,
    down,
    ydstogo,
    yardline_100,
    play_type,
    yards_gained,
    touchdown
FROM bronze.nfl_play_by_play
WHERE yardline_100 <= 20
  AND yardline_100 IS NOT NULL
  AND play_type IN ('pass', 'run')
  AND season = 2023
LIMIT 30;


-- Query 4: Find all 3rd down conversion attempts
-- Try modifying: Look at 4th downs, or add distance filters (ydstogo > 5)
SELECT 
    posteam,
    down,
    ydstogo,
    play_type,
    yards_gained,
    first_down,
    CASE 
        WHEN first_down = true THEN 'Converted'
        ELSE 'Failed'
    END as conversion_result
FROM bronze.nfl_play_by_play
WHERE down = 3
  AND play_type IN ('pass', 'run')
  AND season = 2023
  AND week = 1
ORDER BY game_id, play_id
LIMIT 30;


-- Query 5: Find all big plays (gains of 20+ yards)
-- Try modifying: Change threshold, or filter by play_type
SELECT 
    week,
    posteam,
    play_type,
    yards_gained,
    passer_player_name,
    receiver_player_name,
    rusher_player_name,
    play_desc
FROM bronze.nfl_play_by_play
WHERE yards_gained >= 20
  AND play_type IN ('pass', 'run')
  AND season = 2023
ORDER BY yards_gained DESC
LIMIT 25;


-- PRACTICE CHALLENGES:
-- 1. Find all interceptions thrown by a specific QB
-- 2. Find all plays in overtime (qtr = 5)
-- 3. Find all plays by your favorite team as posteam
-- 4. Find all sacks that resulted in a fumble
-- 5. Find all passing TDs of 40+ yards