-- ============================================================================
-- Testing Player Opportunity Transformation
-- Validate the silver layer transformation before running it
-- ============================================================================

-- First, let's check what data we have in our source tables
-- This helps understand if we're ready to run the transformation

-- ============================================================================
-- STEP 1: Validate Source Data Availability
-- ============================================================================

-- Check snap counts
SELECT 
    'Snap Counts' as source,
    COUNT(*) as records,
    COUNT(DISTINCT player) as players,
    MIN(season) as min_season,
    MAX(season) as max_season,
    COUNT(DISTINCT week) as weeks
FROM bronze.nfl_snap_counts;

-- Check play-by-play
SELECT 
    'Play-by-Play' as source,
    COUNT(*) as records,
    COUNT(DISTINCT receiver_player_name) as receivers,
    COUNT(DISTINCT rusher_player_name) as rushers,
    MIN(season) as min_season,
    MAX(season) as max_season
FROM bronze.nfl_play_by_play;

-- Check NGS tables
SELECT 'NGS Passing' as source, COUNT(*) as records FROM bronze.nfl_ngs_passing
UNION ALL
SELECT 'NGS Rushing', COUNT(*) FROM bronze.nfl_ngs_rushing
UNION ALL
SELECT 'NGS Receiving', COUNT(*) FROM bronze.nfl_ngs_receiving;

-- ============================================================================
-- STEP 2: Test Individual Components
-- ============================================================================

-- Test 1: Verify target share calculation
-- Pick a team and week to validate percentages add up
WITH team_validation AS (
    SELECT 
        receiver_player_name,
        COUNT(*) as targets,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as target_share
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true 
      AND receiver_player_name IS NOT NULL
      AND posteam = 'BUF'  -- Pick a team
      AND season = 2023
      AND week = 1
    GROUP BY receiver_player_name
)
SELECT 
    *,
    SUM(target_share) OVER () as total_share  -- Should be ~100%
FROM team_validation
ORDER BY targets DESC;

-- Test 2: Verify carry share calculation
WITH carry_validation AS (
    SELECT 
        rusher_player_name,
        COUNT(*) as carries,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as carry_share
    FROM bronze.nfl_play_by_play
    WHERE rush_attempt = true 
      AND rusher_player_name IS NOT NULL
      AND posteam = 'SF'  -- Pick a team with good RB usage
      AND season = 2023
      AND week = 1
    GROUP BY rusher_player_name
)
SELECT 
    *,
    SUM(carry_share) OVER () as total_share  -- Should be 100%
FROM carry_validation
ORDER BY carries DESC;

-- Test 3: Check red zone opportunities
SELECT 
    'Red Zone Plays' as category,
    COUNT(CASE WHEN pass_attempt = true THEN 1 END) as rz_passes,
    COUNT(CASE WHEN rush_attempt = true THEN 1 END) as rz_rushes,
    COUNT(*) as total_rz_plays
FROM bronze.nfl_play_by_play
WHERE yardline_100 <= 20
  AND season = 2023
  AND week = 1;

-- ============================================================================
-- STEP 3: Test the Full Transformation (Sample)
-- ============================================================================

-- Run a simplified version of the transformation for one player
-- This helps validate the logic before running the full query

WITH test_player AS (
    SELECT 'Josh Allen' as test_name  -- Change to test different players
),
snap_data AS (
    SELECT 
        player,
        season,
        week,
        offense_snaps as snap_count,
        offense_pct as snap_pct
    FROM bronze.nfl_snap_counts
    WHERE player = (SELECT test_name FROM test_player)
      AND season = 2023
      AND week = 1
),
target_data AS (
    SELECT 
        receiver_player_name as player,
        COUNT(*) as targets,
        SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as receptions
    FROM bronze.nfl_play_by_play
    WHERE receiver_player_name = (SELECT test_name FROM test_player)
      AND season = 2023
      AND week = 1
    GROUP BY receiver_player_name
),
carry_data AS (
    SELECT 
        rusher_player_name as player,
        COUNT(*) as carries,
        SUM(rushing_yards) as rushing_yards
    FROM bronze.nfl_play_by_play
    WHERE rusher_player_name = (SELECT test_name FROM test_player)
      AND season = 2023
      AND week = 1
    GROUP BY rusher_player_name
)
SELECT 
    COALESCE(s.player, t.player, c.player) as player,
    s.snap_count,
    s.snap_pct,
    COALESCE(t.targets, 0) as targets,
    COALESCE(t.receptions, 0) as receptions,
    COALESCE(c.carries, 0) as carries,
    COALESCE(c.rushing_yards, 0) as rushing_yards
FROM snap_data s
FULL OUTER JOIN target_data t ON s.player = t.player
FULL OUTER JOIN carry_data c ON s.player = c.player;

-- ============================================================================
-- STEP 4: Validate Player Name Matching
-- ============================================================================

-- IMPORTANT: Check for name mismatches between data sources
-- This is a common issue that breaks joins

-- Find players in snap counts but not in play-by-play
WITH snap_players AS (
    SELECT DISTINCT player FROM bronze.nfl_snap_counts WHERE season = 2023
),
pbp_players AS (
    SELECT DISTINCT receiver_player_name as player 
    FROM bronze.nfl_play_by_play 
    WHERE season = 2023 AND receiver_player_name IS NOT NULL
    UNION
    SELECT DISTINCT rusher_player_name 
    FROM bronze.nfl_play_by_play 
    WHERE season = 2023 AND rusher_player_name IS NOT NULL
)
SELECT 
    'In Snaps but not PBP' as issue,
    s.player
FROM snap_players s
LEFT JOIN pbp_players p ON s.player = p.player
WHERE p.player IS NULL
LIMIT 10;

-- ============================================================================
-- STEP 5: Run the Full Transformation (Limited)
-- ============================================================================

-- YOUR TURN: Once you've validated the components, run the full transformation
-- Start with a small subset (1 week) to verify it works

-- Uncomment and run this to test the full transformation:
/*
INSERT INTO silver.player_opportunity
WITH ... -- Copy the full transformation query here
SELECT * FROM combined_data
WHERE season = 2023
  AND week = 1
LIMIT 100;  -- Start with limited rows
*/

-- Then verify the results:
/*
SELECT * FROM silver.player_opportunity
WHERE season = 2023 AND week = 1
ORDER BY snap_count DESC
LIMIT 20;
*/

-- ============================================================================
-- STEP 6: Data Quality Checks
-- ============================================================================

-- After running the transformation, validate the output:

-- Check 1: Are shares reasonable? (should be between 0-100)
/*
SELECT 
    MIN(target_share) as min_target_share,
    MAX(target_share) as max_target_share,
    MIN(carry_share) as min_carry_share,
    MAX(carry_share) as max_carry_share
FROM silver.player_opportunity;
*/

-- Check 2: Do we have duplicate records?
/*
SELECT 
    player_id, season, week, 
    COUNT(*) as record_count
FROM silver.player_opportunity
GROUP BY player_id, season, week
HAVING COUNT(*) > 1;
*/

-- Check 3: Compare known player stats
-- Pick a well-known player and verify their Week 1 stats match expectations
/*
SELECT *
FROM silver.player_opportunity
WHERE player_id LIKE '%McCaffrey%'
  AND season = 2023
  AND week = 1;
*/

-- ============================================================================
-- YOUR ANALYSIS: Add your own validation queries here
-- ============================================================================