-- ============================================
-- SQL Practice: Joins with Play-by-Play and Player Data
-- ============================================
-- Purpose: Learn different types of JOINs using NFL data
-- Concepts: INNER JOIN, LEFT JOIN, self-joins, multiple joins
-- ============================================

-- Query 1: Join play-by-play with player info for QBs
-- Try modifying: Join on receiver or rusher instead
SELECT 
    pbp.week,
    pbp.passer_player_name,
    p.team,
    p.college,
    COUNT(*) as attempts,
    SUM(pbp.passing_yards) as total_yards
FROM bronze.nfl_play_by_play pbp
INNER JOIN bronze.nfl_players p 
    ON pbp.passer_player_id = p.player_id
WHERE pbp.season = 2023
  AND pbp.pass_attempt = true
GROUP BY pbp.week, pbp.passer_player_name, p.team, p.college
ORDER BY pbp.week, total_yards DESC
LIMIT 30;


-- Query 2: Compare player performance to their season average
-- Self-join example
WITH player_season_avg AS (
    SELECT 
        passer_player_name,
        AVG(passing_yards) as season_avg_yards
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true
      AND season = 2023
      AND passer_player_name IS NOT NULL
    GROUP BY passer_player_name
    HAVING COUNT(*) >= 100
)
SELECT 
    pbp.week,
    pbp.passer_player_name,
    SUM(pbp.passing_yards) as week_yards,
    ROUND(psa.season_avg_yards, 1) as season_avg,
    ROUND(SUM(pbp.passing_yards) - psa.season_avg_yards, 1) as vs_average
FROM bronze.nfl_play_by_play pbp
INNER JOIN player_season_avg psa
    ON pbp.passer_player_name = psa.passer_player_name
WHERE pbp.pass_attempt = true
  AND pbp.season = 2023
GROUP BY pbp.week, pbp.passer_player_name, psa.season_avg_yards
HAVING SUM(pbp.passing_yards) IS NOT NULL
ORDER BY vs_average DESC
LIMIT 25;


-- Query 3: Find head-to-head matchups between teams
-- Try modifying: Look at specific teams or divisional games
SELECT 
    home.week,
    home.home_team,
    home.away_team,
    COUNT(DISTINCT home.game_id) as game_id_check,
    SUM(CASE WHEN home.posteam = home.home_team THEN home.yards_gained ELSE 0 END) as home_total_yards,
    SUM(CASE WHEN home.posteam = home.away_team THEN home.yards_gained ELSE 0 END) as away_total_yards
FROM bronze.nfl_play_by_play home
WHERE home.season = 2023
  AND home.play_type IN ('pass', 'run')
GROUP BY home.week, home.home_team, home.away_team
ORDER BY home.week
LIMIT 30;


-- Query 4: Player performance with game context
-- Multiple joins to get complete picture
SELECT 
    pbp.week,
    pbp.game_id,
    pbp.receiver_player_name,
    p.position,
    pbp.posteam,
    pbp.defteam,
    COUNT(*) as targets,
    SUM(CASE WHEN pbp.complete_pass = true THEN 1 ELSE 0 END) as catches,
    SUM(pbp.receiving_yards) as yards,
    MAX(pbp.receiving_yards) as longest
FROM bronze.nfl_play_by_play pbp
LEFT JOIN bronze.nfl_players p
    ON pbp.receiver_player_id = p.player_id
WHERE pbp.receiver_player_name IS NOT NULL
  AND pbp.season = 2023
  AND pbp.week <= 4
GROUP BY pbp.week, pbp.game_id, pbp.receiver_player_name, 
         p.position, pbp.posteam, pbp.defteam
HAVING COUNT(*) >= 3  -- At least 3 targets in a game
ORDER BY yards DESC
LIMIT 30;


-- Query 5: Compare offensive and defensive performance
-- Using subqueries and joins
WITH offensive_stats AS (
    SELECT 
        posteam as team,
        AVG(yards_gained) as avg_yards_gained,
        AVG(epa) as avg_epa
    FROM bronze.nfl_play_by_play
    WHERE season = 2023
      AND play_type IN ('pass', 'run')
      AND posteam IS NOT NULL
    GROUP BY posteam
),
defensive_stats AS (
    SELECT 
        defteam as team,
        AVG(yards_gained) as avg_yards_allowed,
        AVG(epa) as avg_epa_allowed
    FROM bronze.nfl_play_by_play
    WHERE season = 2023
      AND play_type IN ('pass', 'run')
      AND defteam IS NOT NULL
    GROUP BY defteam
)
SELECT 
    o.team,
    ROUND(o.avg_yards_gained, 2) as off_yards_per_play,
    ROUND(d.avg_yards_allowed, 2) as def_yards_allowed,
    ROUND(o.avg_epa, 3) as offensive_epa,
    ROUND(d.avg_epa_allowed, 3) as defensive_epa,
    ROUND(o.avg_epa - d.avg_epa_allowed, 3) as net_epa
FROM offensive_stats o
INNER JOIN defensive_stats d
    ON o.team = d.team
ORDER BY net_epa DESC;


-- PRACTICE CHALLENGES:
-- 1. Join to find all TDs scored by rookies (using draft_year from players table)
-- 2. Compare home vs away performance for each team
-- 3. Find QB-WR combinations with the most connections
-- 4. Join to show player performance against specific defenses
-- 5. Create a query showing player stats with their team's record