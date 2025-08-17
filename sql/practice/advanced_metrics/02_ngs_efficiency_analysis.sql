-- ============================================================================
-- Next Gen Stats (NGS) Efficiency Analysis
-- Learn to identify efficient players using advanced metrics
-- ============================================================================

-- ============================================================================
-- PART 1: RUSHING EFFICIENCY
-- ============================================================================

-- First, explore the NGS rushing data
SELECT *
FROM bronze.nfl_ngs_rushing
WHERE season = 2023 
  AND week = 1
LIMIT 5;

-- CHALLENGE 1: Find the most efficient rushers
-- Which RBs consistently beat expected rushing yards?
-- YOUR TURN: Complete this query
SELECT 
    player_display_name,
    team_abbr,
    rush_attempts,
    rush_yards,
    rush_yards_over_expected,
    rush_yards_over_expected_per_att,
    -- TODO: Calculate efficiency rating (yards over expected / attempts)
    -- TODO: Add a CASE statement to categorize efficiency (Elite/Good/Average/Poor)
FROM bronze.nfl_ngs_rushing
WHERE season = 2023
  AND rush_attempts >= 10  -- Minimum volume threshold
ORDER BY rush_yards_over_expected_per_att DESC
LIMIT 15;

-- CHALLENGE 2: Stacked Box Analysis
-- Which RBs face the toughest defensive fronts but still produce?
WITH tough_runners AS (
    SELECT 
        player_display_name,
        AVG(percent_attempts_gte_eight_defenders) as avg_stacked_box_rate,
        AVG(rush_yards_over_expected_per_att) as avg_yoe_per_att,
        SUM(rush_attempts) as total_attempts,
        -- TODO: Calculate "toughness score" combining stacked box rate and efficiency
    FROM bronze.nfl_ngs_rushing
    WHERE season = 2023
    GROUP BY player_display_name
    HAVING SUM(rush_attempts) >= 50  -- Minimum volume
)
SELECT 
    *,
    -- TODO: Rank by your toughness score
FROM tough_runners
ORDER BY avg_stacked_box_rate DESC;

-- ============================================================================
-- PART 2: PASSING EFFICIENCY
-- ============================================================================

-- Explore NGS passing data
SELECT *
FROM bronze.nfl_ngs_passing
WHERE season = 2023 
  AND week = 1
LIMIT 5;

-- CHALLENGE 3: Aggressive vs Efficient QBs
-- Find QBs who take risks (high aggressiveness) but complete passes
SELECT 
    player_display_name,
    team_abbr,
    attempts,
    aggressiveness,  -- Throws into tight windows
    completion_percentage,
    completion_percentage_above_expectation as cpoe,
    avg_time_to_throw,
    -- TODO: Create an "aggressive efficiency" score
    -- Hint: Combine aggressiveness with CPOE
    -- TODO: Classify QB style (Gunslinger/Game Manager/Balanced)
FROM bronze.nfl_ngs_passing
WHERE season = 2023
  AND attempts >= 20
ORDER BY aggressiveness DESC;

-- CHALLENGE 4: Pressure Performance
-- Which QBs maintain efficiency despite quick throws?
WITH qb_under_pressure AS (
    SELECT 
        player_display_name,
        AVG(avg_time_to_throw) as avg_ttt,
        AVG(completion_percentage_above_expectation) as avg_cpoe,
        AVG(avg_air_yards_differential) as avg_ayd,
        SUM(attempts) as total_attempts,
        -- TODO: Identify "quick release" QBs (low time to throw)
        -- TODO: Calculate their efficiency when throwing quickly
    FROM bronze.nfl_ngs_passing
    WHERE season = 2023
    GROUP BY player_display_name
    HAVING SUM(attempts) >= 100
)
SELECT 
    *,
    -- TODO: Create a "pressure rating" for QBs who throw quickly but accurately
FROM qb_under_pressure
WHERE avg_ttt < 2.7  -- Quick release threshold
ORDER BY avg_cpoe DESC;

-- ============================================================================
-- PART 3: RECEIVING SEPARATION
-- ============================================================================

-- Explore NGS receiving data
SELECT *
FROM bronze.nfl_ngs_receiving
WHERE season = 2023 
  AND week = 1
LIMIT 5;

-- CHALLENGE 5: Separation Kings
-- Which receivers consistently get open?
SELECT 
    player_display_name,
    position,
    team_abbr,
    targets,
    receptions,
    avg_separation,
    avg_cushion,
    percent_share_of_intended_air_yards,
    -- TODO: Calculate "openness score" using separation and cushion
    -- TODO: Identify route runners vs physical receivers
FROM bronze.nfl_ngs_receiving
WHERE season = 2023
  AND targets >= 5
ORDER BY avg_separation DESC
LIMIT 20;

-- CHALLENGE 6: YAC Monsters
-- Find receivers who create yards after catch
WITH yac_leaders AS (
    SELECT 
        player_display_name,
        position,
        AVG(avg_yac) as avg_yac,
        AVG(avg_expected_yac) as avg_expected_yac,
        AVG(avg_yac_above_expectation) as avg_yacoe,
        SUM(receptions) as total_receptions,
        -- TODO: Calculate YAC efficiency rating
    FROM bronze.nfl_ngs_receiving
    WHERE season = 2023
    GROUP BY player_display_name, position
    HAVING SUM(receptions) >= 20
)
SELECT 
    *,
    -- TODO: Rank by YAC creation ability
    -- TODO: Compare TEs vs WRs vs RBs
FROM yac_leaders
ORDER BY avg_yacoe DESC;

-- ============================================================================
-- PART 4: CROSS-POSITION ANALYSIS
-- ============================================================================

-- CHALLENGE 7: Complete Offensive Efficiency
-- Combine all NGS data to find the most efficient offensive players

-- YOUR TURN: Create a query that:
-- 1. Unions data from all three NGS tables
-- 2. Calculates position-adjusted efficiency scores
-- 3. Identifies the top 5 most efficient players at each position

WITH all_efficiency AS (
    -- TODO: Combine rushing efficiency data
    SELECT 
        player_display_name,
        'RB' as position,
        rush_yards_over_expected_per_att as efficiency_score,
        'Rushing YOE' as metric_type
    FROM bronze.nfl_ngs_rushing
    WHERE season = 2023
    
    UNION ALL
    
    -- TODO: Add passing efficiency data
    
    UNION ALL
    
    -- TODO: Add receiving efficiency data
)
SELECT 
    *,
    -- TODO: Add position-specific rankings
FROM all_efficiency;

-- ============================================================================
-- BONUS CHALLENGES
-- ============================================================================

-- 1. Create a "Hidden Gems" query that finds:
--    - Players with high efficiency but low volume
--    - Could be league-winners if given more opportunity

-- 2. Build a "Regression Candidates" analysis:
--    - Players whose efficiency metrics are unsustainably high/low
--    - Use standard deviation to find outliers

-- 3. Design a "Matchup Advantage" system:
--    - Compare player efficiency vs opponent averages
--    - Identify favorable weekly matchups

-- YOUR BONUS ANALYSIS HERE: