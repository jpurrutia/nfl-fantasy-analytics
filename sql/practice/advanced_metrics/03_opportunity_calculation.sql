-- ============================================================================
-- Building the Silver Layer: Player Opportunity Calculation
-- Learn how to transform raw data into actionable opportunity metrics
-- ============================================================================

-- This file teaches you how to build the silver.player_opportunity table
-- by combining data from multiple bronze sources

-- ============================================================================
-- STEP 1: Understanding the Source Data
-- ============================================================================

-- First, let's verify all our data sources are available
-- Run each query to understand what data we have:

-- Snap counts
SELECT COUNT(*) as snap_records, 
       COUNT(DISTINCT player) as unique_players,
       MIN(week) as min_week, 
       MAX(week) as max_week
FROM bronze.nfl_snap_counts
WHERE season = 2023;

-- Play-by-play for targets/carries
SELECT COUNT(*) as plays,
       COUNT(DISTINCT receiver_player_name) as receivers,
       COUNT(DISTINCT rusher_player_name) as rushers
FROM bronze.nfl_play_by_play
WHERE season = 2023;

-- NGS data
SELECT 'Passing' as type, COUNT(*) as records FROM bronze.nfl_ngs_passing WHERE season = 2023
UNION ALL
SELECT 'Rushing', COUNT(*) FROM bronze.nfl_ngs_rushing WHERE season = 2023
UNION ALL
SELECT 'Receiving', COUNT(*) FROM bronze.nfl_ngs_receiving WHERE season = 2023;

-- ============================================================================
-- STEP 2: Calculate Targets and Target Share
-- ============================================================================

-- CHALLENGE 1: Build target share calculation
-- YOUR TURN: Complete this query to calculate weekly target shares

WITH team_targets AS (
    -- First, calculate total team targets per game
    SELECT 
        posteam as team,
        season,
        week,
        COUNT(*) as team_total_targets
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true 
      AND receiver_player_name IS NOT NULL
      AND season = 2023
    GROUP BY posteam, season, week
),
player_targets AS (
    -- Then calculate individual player targets
    SELECT 
        receiver_player_name as player,
        posteam as team,
        season,
        week,
        COUNT(*) as player_targets,
        -- TODO: Add receptions count
        -- Hint: SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END)
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true 
      AND receiver_player_name IS NOT NULL
      AND season = 2023
    GROUP BY receiver_player_name, posteam, season, week
)
SELECT 
    p.player,
    p.team,
    p.week,
    p.player_targets,
    t.team_total_targets,
    -- TODO: Calculate target share (player_targets / team_total_targets * 100)
    -- TODO: Rank players by target share within their team
FROM player_targets p
JOIN team_targets t
    ON p.team = t.team 
    AND p.season = t.season 
    AND p.week = t.week
WHERE p.week = 1  -- Start with week 1
ORDER BY p.team, p.player_targets DESC;

-- ============================================================================
-- STEP 3: Calculate Red Zone Opportunities
-- ============================================================================

-- CHALLENGE 2: Identify red zone usage patterns
-- YOUR TURN: Find red zone target and carry leaders

WITH rz_opportunities AS (
    SELECT 
        COALESCE(receiver_player_name, rusher_player_name) as player,
        posteam as team,
        season,
        week,
        -- TODO: Count red zone targets (pass plays in red zone)
        SUM(CASE WHEN pass_attempt = true AND yardline_100 <= 20 
                 AND receiver_player_name IS NOT NULL THEN 1 ELSE 0 END) as rz_targets,
        -- TODO: Count red zone carries
        SUM(CASE WHEN rush_attempt = true AND yardline_100 <= 20 
                 AND rusher_player_name IS NOT NULL THEN 1 ELSE 0 END) as rz_carries,
        -- TODO: Calculate total red zone touches
    FROM bronze.nfl_play_by_play
    WHERE season = 2023
      AND (receiver_player_name IS NOT NULL OR rusher_player_name IS NOT NULL)
    GROUP BY COALESCE(receiver_player_name, rusher_player_name), posteam, season, week
)
SELECT 
    player,
    team,
    SUM(rz_targets) as total_rz_targets,
    SUM(rz_carries) as total_rz_carries,
    -- TODO: Calculate total red zone opportunities
    -- TODO: Identify TD conversion rate from red zone touches
FROM rz_opportunities
GROUP BY player, team
HAVING (SUM(rz_targets) + SUM(rz_carries)) > 5  -- Minimum threshold
ORDER BY (SUM(rz_targets) + SUM(rz_carries)) DESC
LIMIT 20;

-- ============================================================================
-- STEP 4: Combine Snap Counts with Usage
-- ============================================================================

-- CHALLENGE 3: Calculate opportunity per snap
-- This shows efficiency of opportunity when on field

WITH snap_and_usage AS (
    SELECT 
        s.player,
        s.team,
        s.week,
        s.offense_snaps,
        s.offense_pct,
        -- Get targets from play-by-play (you'll need to join)
        -- TODO: Add target count
        -- TODO: Add carry count
        -- TODO: Calculate opportunities per snap
    FROM bronze.nfl_snap_counts s
    WHERE s.season = 2023
      AND s.position IN ('RB', 'WR', 'TE')
      AND s.offense_snaps > 10
)
-- TODO: Complete the join with play-by-play data
SELECT * FROM snap_and_usage
LIMIT 10;

-- ============================================================================
-- STEP 5: Add Position-Specific Metrics
-- ============================================================================

-- CHALLENGE 4: Integrate NGS efficiency data
-- Combine opportunity with efficiency for complete picture

-- For RBs: Combine carries with yards over expected
WITH rb_opportunity AS (
    SELECT 
        r.player_display_name as player,
        r.week,
        r.rush_attempts,
        r.rush_yards_over_expected_per_att,
        r.percent_attempts_gte_eight_defenders as stacked_box_rate,
        -- TODO: Join with snap counts to get snap share
        -- TODO: Join with play-by-play to get red zone carries
    FROM bronze.nfl_ngs_rushing r
    WHERE r.season = 2023
      AND r.rush_attempts >= 5
)
SELECT * FROM rb_opportunity
WHERE week = 1
ORDER BY rush_attempts DESC;

-- For WRs: Combine targets with separation metrics
WITH wr_opportunity AS (
    SELECT 
        w.player_display_name as player,
        w.week,
        w.targets,
        w.receptions,
        w.avg_separation,
        w.percent_share_of_intended_air_yards,
        -- TODO: Calculate catch rate
        -- TODO: Add route participation (if available)
    FROM bronze.nfl_ngs_receiving w
    WHERE w.season = 2023
      AND w.targets >= 3
)
SELECT * FROM wr_opportunity
WHERE week = 1
ORDER BY targets DESC;

-- ============================================================================
-- STEP 6: Build the Complete Opportunity Table
-- ============================================================================

-- CHALLENGE 5: Create the full silver.player_opportunity insert
-- This is the actual transformation that populates our silver layer

-- YOUR TURN: Complete this comprehensive query
WITH 
-- Snap data
snap_data AS (
    SELECT 
        player,
        season,
        week,
        team,
        position,
        offense_snaps as snap_count,
        offense_pct as snap_pct
    FROM bronze.nfl_snap_counts
    WHERE season = 2023
),

-- Target and carry data from play-by-play
usage_data AS (
    -- TODO: Calculate targets, carries, receptions per player/week
    SELECT 
        receiver_player_name as player,
        season,
        week,
        COUNT(*) as targets
        -- Add more metrics
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true
    GROUP BY receiver_player_name, season, week
),

-- Red zone data
rz_data AS (
    -- TODO: Calculate red zone opportunities
    SELECT 
        player,
        season,
        week,
        0 as rz_targets  -- Replace with actual calculation
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
),

-- NGS efficiency data
ngs_data AS (
    -- TODO: Combine all three NGS tables
    SELECT 
        player_display_name as player,
        season,
        week,
        0.0 as efficiency_score  -- Replace with actual metrics
    FROM bronze.nfl_ngs_receiving
)

-- Final assembly
SELECT 
    COALESCE(s.player, u.player) as player_id,
    COALESCE(s.season, u.season) as season,
    COALESCE(s.week, u.week) as week,
    s.position,
    s.team,
    s.snap_count,
    s.snap_pct,
    u.targets,
    -- TODO: Add all other opportunity metrics
    -- TODO: Add calculated fields (target share, etc.)
    CURRENT_TIMESTAMP as created_at,
    CURRENT_TIMESTAMP as updated_at
FROM snap_data s
FULL OUTER JOIN usage_data u 
    ON s.player = u.player 
    AND s.season = u.season 
    AND s.week = u.week
-- TODO: Add other joins
WHERE COALESCE(s.week, u.week) = 1  -- Test with week 1 first
LIMIT 20;

-- ============================================================================
-- STEP 7: Validate Your Results
-- ============================================================================

-- VALIDATION 1: Check for data completeness
-- Are we missing any players who had snaps but no targets/carries?

-- VALIDATION 2: Sanity check the calculations
-- Do target shares add up to ~100% per team?

-- VALIDATION 3: Compare with known values
-- Pick a well-known player and verify their metrics match expectations

-- ============================================================================
-- BONUS: Create Your Own Opportunity Metrics
-- ============================================================================

-- Ideas for custom opportunity metrics:
-- 1. "High-value touches" - RZ targets + carries on 3rd down
-- 2. "Deep ball share" - Percentage of team's 20+ yard targets
-- 3. "Goal line back" - Carries inside the 5-yard line
-- 4. "Slot vs outside" - Where are WR targets coming from?

-- YOUR CUSTOM METRIC HERE: