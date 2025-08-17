-- ============================================
-- SQL Practice: Window Functions with Play-by-Play Data
-- ============================================
-- Purpose: Learn window functions for advanced analytics
-- Concepts: ROW_NUMBER, RANK, LAG/LEAD, running totals, PARTITION BY
-- ============================================

-- Query 1: Rank QBs by passing yards each week
-- Try modifying: Rank by TDs, or use DENSE_RANK instead of RANK
SELECT 
    week,
    passer_player_name,
    SUM(passing_yards) as week_yards,
    RANK() OVER (PARTITION BY week ORDER BY SUM(passing_yards) DESC) as week_rank,
    RANK() OVER (ORDER BY SUM(passing_yards) DESC) as overall_rank
FROM bronze.nfl_play_by_play
WHERE pass_attempt = true
  AND season = 2023
  AND passer_player_name IS NOT NULL
GROUP BY week, passer_player_name
HAVING SUM(passing_yards) > 0
ORDER BY week, week_rank
LIMIT 50;


-- Query 2: Running totals for player stats
-- Try modifying: Calculate for rushing yards or receptions
SELECT 
    week,
    passer_player_name,
    SUM(passing_yards) as week_yards,
    SUM(pass_touchdown) as week_tds,
    SUM(SUM(passing_yards)) OVER (
        PARTITION BY passer_player_name 
        ORDER BY week 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as season_yards_to_date,
    SUM(SUM(pass_touchdown)) OVER (
        PARTITION BY passer_player_name 
        ORDER BY week 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as season_tds_to_date
FROM bronze.nfl_play_by_play
WHERE pass_attempt = true
  AND season = 2023
  AND passer_player_name IN ('D.Prescott', 'J.Allen', 'P.Mahomes', 'J.Hurts')
GROUP BY week, passer_player_name
ORDER BY passer_player_name, week;


-- Query 3: Compare current play to previous play (LAG function)
-- Try modifying: Use LEAD to look at next play, or change the offset
WITH play_sequence AS (
    SELECT 
        game_id,
        play_id,
        posteam,
        down,
        ydstogo,
        yards_gained,
        play_type,
        LAG(yards_gained, 1) OVER (
            PARTITION BY game_id, posteam 
            ORDER BY play_id
        ) as previous_play_yards,
        LAG(play_type, 1) OVER (
            PARTITION BY game_id, posteam 
            ORDER BY play_id
        ) as previous_play_type
    FROM bronze.nfl_play_by_play
    WHERE season = 2023
      AND week = 1
      AND play_type IN ('pass', 'run')
)
SELECT 
    game_id,
    posteam,
    down,
    ydstogo,
    previous_play_type,
    previous_play_yards,
    play_type as current_play_type,
    yards_gained as current_yards,
    yards_gained - COALESCE(previous_play_yards, 0) as yard_difference
FROM play_sequence
WHERE previous_play_yards IS NOT NULL
ORDER BY game_id, play_id
LIMIT 50;


-- Query 4: Moving averages for player performance
-- Try modifying: Change window size or calculate for different stats
SELECT 
    week,
    receiver_player_name,
    COUNT(*) as targets,
    SUM(receiving_yards) as week_yards,
    ROUND(AVG(SUM(receiving_yards)) OVER (
        PARTITION BY receiver_player_name 
        ORDER BY week 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 1) as three_week_avg,
    ROUND(AVG(SUM(receiving_yards)) OVER (
        PARTITION BY receiver_player_name 
        ORDER BY week 
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ), 1) as five_week_avg
FROM bronze.nfl_play_by_play
WHERE receiver_player_name IN ('T.Hill', 'C.Lamb', 'A.Brown', 'J.Jefferson')
  AND pass_attempt = true
  AND season = 2023
GROUP BY week, receiver_player_name
ORDER BY receiver_player_name, week;


-- Query 5: Percentile rankings for big plays
-- Try modifying: Look at different percentiles or yard thresholds
WITH big_plays AS (
    SELECT 
        posteam,
        play_id,
        play_type,
        yards_gained,
        PERCENT_RANK() OVER (ORDER BY yards_gained) as percentile_rank,
        NTILE(100) OVER (ORDER BY yards_gained) as percentile_bucket
    FROM bronze.nfl_play_by_play
    WHERE season = 2023
      AND play_type IN ('pass', 'run')
      AND yards_gained > 0
)
SELECT 
    posteam,
    COUNT(*) as total_plays,
    SUM(CASE WHEN percentile_rank >= 0.90 THEN 1 ELSE 0 END) as top_10_pct_plays,
    SUM(CASE WHEN percentile_rank >= 0.95 THEN 1 ELSE 0 END) as top_5_pct_plays,
    SUM(CASE WHEN percentile_rank >= 0.99 THEN 1 ELSE 0 END) as top_1_pct_plays,
    MAX(yards_gained) as longest_play
FROM big_plays
GROUP BY posteam
ORDER BY top_10_pct_plays DESC;


-- Query 6: First and last occurrences using window functions
-- Try modifying: Find first TD, or last play of each quarter
SELECT DISTINCT
    passer_player_name,
    FIRST_VALUE(week) OVER (
        PARTITION BY passer_player_name 
        ORDER BY week
    ) as first_game_week,
    LAST_VALUE(week) OVER (
        PARTITION BY passer_player_name 
        ORDER BY week
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) as last_game_week,
    COUNT(*) OVER (PARTITION BY passer_player_name) as total_attempts
FROM bronze.nfl_play_by_play
WHERE pass_attempt = true
  AND season = 2023
  AND passer_player_name IS NOT NULL
ORDER BY total_attempts DESC
LIMIT 30;


-- PRACTICE CHALLENGES:
-- 1. Calculate the cumulative EPA for each team throughout a game
-- 2. Rank teams by their 3rd down conversion rate each week
-- 3. Find the longest streak of successful plays (positive EPA) for each team
-- 4. Calculate rolling 4-week averages for team scoring
-- 5. Use ROW_NUMBER to find each player's best and worst games