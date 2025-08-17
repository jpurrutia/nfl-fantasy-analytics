-- ============================================================================
-- Snap Count Data Exploration
-- Learn to analyze player usage patterns with snap count data
-- ============================================================================

-- First, let's understand the data structure
-- TODO: Run this query to see what snap count data looks like
SELECT *
FROM bronze.nfl_snap_counts
WHERE season = 2023 
  AND week = 1
LIMIT 5;

-- ============================================================================
-- CHALLENGE 1: Find the workhorses
-- Which RBs had the highest snap percentage in Week 1, 2023?
-- ============================================================================

-- YOUR TURN: Complete this query
-- Hint: Filter by position = 'RB' and look at offense_pct
SELECT 
    player,
    team,
    offense_snaps,
    offense_pct,
    -- TODO: Add a RANK() window function to rank by snap percentage
FROM bronze.nfl_snap_counts
WHERE season = 2023 
  AND week = 1
  AND position = 'RB'
  AND offense_snaps > 0
ORDER BY offense_pct DESC
LIMIT 10;

-- ============================================================================
-- CHALLENGE 2: Snap count trends
-- Track a player's snap count progression over the season
-- ============================================================================

-- YOUR TURN: Pick a player and analyze their usage trend
-- Example: How did Christian McCaffrey's usage change throughout 2023?
WITH player_snaps AS (
    SELECT 
        week,
        offense_snaps,
        offense_pct,
        -- TODO: Add a moving average using AVG() OVER window function
        -- Hint: Use ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    FROM bronze.nfl_snap_counts
    WHERE player = 'Christian McCaffrey'  -- Change this to any player
      AND season = 2023
    ORDER BY week
)
SELECT * FROM player_snaps;

-- ============================================================================
-- CHALLENGE 3: Team snap distribution
-- How do teams distribute snaps among their RBs?
-- ============================================================================

-- YOUR TURN: Calculate the snap share distribution for each team's backfield
WITH rb_snaps AS (
    SELECT 
        team,
        player,
        SUM(offense_snaps) as total_snaps,
        -- TODO: Calculate each RB's share of team RB snaps
        -- Hint: Use SUM() OVER (PARTITION BY team)
    FROM bronze.nfl_snap_counts
    WHERE position = 'RB'
      AND season = 2023
    GROUP BY team, player
)
SELECT 
    team,
    player,
    total_snaps,
    -- TODO: Add snap_share percentage
    -- TODO: Add a classification (lead back, committee, backup)
FROM rb_snaps
ORDER BY team, total_snaps DESC;

-- ============================================================================
-- CHALLENGE 4: Snap count vs fantasy production correlation
-- Do more snaps = more fantasy points?
-- ============================================================================

-- YOUR TURN: Join snap counts with performance data
SELECT 
    s.player,
    s.week,
    s.offense_snaps,
    s.offense_pct,
    p.fantasy_points_ppr,
    -- TODO: Calculate fantasy points per snap
    -- TODO: Add correlation analysis
FROM bronze.nfl_snap_counts s
JOIN bronze.nfl_player_performance p
    ON s.player = p.player_id  -- Note: May need name matching
    AND s.season = p.season
    AND s.week = p.week
WHERE s.position IN ('RB', 'WR', 'TE')
  AND s.season = 2023
  AND s.week BETWEEN 1 AND 5;

-- ============================================================================
-- CHALLENGE 5: Identify breakout candidates
-- Find players with increasing snap counts but low roster percentages
-- ============================================================================

-- YOUR TURN: This is an advanced query combining multiple concepts
WITH snap_trends AS (
    SELECT 
        player,
        position,
        team,
        week,
        offense_pct,
        -- TODO: Calculate week-over-week change in snap percentage
        LAG(offense_pct) OVER (PARTITION BY player ORDER BY week) as prev_week_pct,
        -- TODO: Calculate 3-week rolling average
    FROM bronze.nfl_snap_counts
    WHERE season = 2023
      AND position IN ('WR', 'RB')
)
SELECT 
    player,
    position,
    team,
    -- TODO: Calculate average snap % increase over last 3 weeks
    -- TODO: Flag players with consistent increases
FROM snap_trends
WHERE week >= 4  -- Need history for trends
GROUP BY player, position, team
-- TODO: Filter for players showing positive trends
ORDER BY /* your metric */ DESC;

-- ============================================================================
-- BONUS: Create your own analysis!
-- Ideas:
-- 1. Which teams use the most 3-WR sets? (Count snaps with 3+ WRs > 50%)
-- 2. Do rookie RBs see increased snaps as season progresses?
-- 3. What's the relationship between snap count and red zone usage?
-- ============================================================================

-- YOUR ANALYSIS HERE: