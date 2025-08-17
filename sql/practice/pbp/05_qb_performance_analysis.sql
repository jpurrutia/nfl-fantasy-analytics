-- ============================================
-- Real Analysis: QB Performance by Situation
-- ============================================
-- Purpose: Analyze QB performance in different game situations
-- Concepts: Complex filtering, CASE statements, aggregations
-- ============================================

-- Query 1: QB performance under pressure (when sacked or hit)
-- This shows how QBs perform when the pocket breaks down
WITH qb_pressure_stats AS (
    SELECT 
        passer_player_name,
        COUNT(*) as total_dropbacks,
        SUM(CASE WHEN sack = true THEN 1 ELSE 0 END) as times_sacked,
        SUM(CASE WHEN qb_hit = true THEN 1 ELSE 0 END) as times_hit,
        SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as completions,
        SUM(passing_yards) as passing_yards,
        AVG(epa) as avg_epa,
        AVG(cpoe) as avg_cpoe
    FROM bronze.nfl_play_by_play
    WHERE (pass_attempt = true OR sack = true)
      AND season = 2023
      AND passer_player_name IS NOT NULL
    GROUP BY passer_player_name
    HAVING COUNT(*) >= 200
)
SELECT 
    passer_player_name,
    total_dropbacks,
    times_sacked,
    times_hit,
    ROUND(100.0 * times_sacked / total_dropbacks, 1) as sack_rate,
    ROUND(100.0 * completions / (total_dropbacks - times_sacked), 1) as completion_pct,
    ROUND(avg_epa, 3) as avg_epa,
    ROUND(avg_cpoe, 1) as avg_cpoe
FROM qb_pressure_stats
ORDER BY avg_epa DESC
LIMIT 20;


-- Query 2: QB performance by quarter (clutch factor)
-- See how QBs perform in critical 4th quarter situations
SELECT 
    passer_player_name,
    qtr,
    COUNT(*) as attempts,
    ROUND(100.0 * SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) / COUNT(*), 1) as comp_pct,
    ROUND(AVG(passing_yards), 1) as avg_yards,
    SUM(pass_touchdown) as touchdowns,
    SUM(interception) as interceptions,
    ROUND(AVG(epa), 3) as avg_epa,
    ROUND(AVG(CASE WHEN qtr = 4 THEN epa ELSE NULL END), 3) as fourth_qtr_epa
FROM bronze.nfl_play_by_play
WHERE pass_attempt = true
  AND season = 2023
  AND passer_player_name IS NOT NULL
  AND qtr IN (1, 2, 3, 4)
GROUP BY passer_player_name, qtr
HAVING COUNT(*) >= 50
ORDER BY passer_player_name, qtr;


-- Query 3: QB performance on 3rd downs by distance
-- Analyze how QBs handle different 3rd down situations
SELECT 
    passer_player_name,
    CASE 
        WHEN ydstogo <= 3 THEN '3rd & Short (1-3)'
        WHEN ydstogo <= 6 THEN '3rd & Medium (4-6)'
        WHEN ydstogo <= 10 THEN '3rd & Long (7-10)'
        ELSE '3rd & Very Long (11+)'
    END as distance_bucket,
    COUNT(*) as attempts,
    ROUND(100.0 * SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) / COUNT(*), 1) as comp_pct,
    ROUND(100.0 * SUM(CASE WHEN first_down = true THEN 1 ELSE 0 END) / COUNT(*), 1) as conversion_rate,
    ROUND(AVG(air_yards), 1) as avg_air_yards,
    ROUND(AVG(yards_gained), 1) as avg_yards_gained,
    ROUND(AVG(epa), 3) as avg_epa
FROM bronze.nfl_play_by_play
WHERE down = 3
  AND pass_attempt = true
  AND season = 2023
  AND passer_player_name IS NOT NULL
GROUP BY passer_player_name, distance_bucket
HAVING COUNT(*) >= 10
ORDER BY passer_player_name, 
         CASE 
            WHEN ydstogo <= 3 THEN 1
            WHEN ydstogo <= 6 THEN 2
            WHEN ydstogo <= 10 THEN 3
            ELSE 4
         END;


-- Query 4: QB red zone efficiency
-- Critical for fantasy football - who gets TDs in the red zone?
WITH red_zone_stats AS (
    SELECT 
        passer_player_name,
        COUNT(*) as rz_attempts,
        SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as rz_completions,
        SUM(pass_touchdown) as rz_pass_tds,
        SUM(interception) as rz_ints,
        AVG(epa) as rz_epa
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
      AND pass_attempt = true
      AND season = 2023
    GROUP BY passer_player_name
)
SELECT 
    passer_player_name,
    rz_attempts,
    ROUND(100.0 * rz_completions / rz_attempts, 1) as rz_comp_pct,
    rz_pass_tds,
    ROUND(100.0 * rz_pass_tds / rz_attempts, 1) as td_rate,
    rz_ints,
    ROUND(rz_epa, 3) as avg_rz_epa
FROM red_zone_stats
WHERE rz_attempts >= 20
ORDER BY td_rate DESC
LIMIT 25;


-- Query 5: QB performance vs specific defenses
-- See which QBs excel against top defenses
WITH defense_rankings AS (
    SELECT 
        defteam,
        AVG(epa) as def_epa_allowed,
        RANK() OVER (ORDER BY AVG(epa)) as def_rank
    FROM bronze.nfl_play_by_play
    WHERE season = 2023
      AND play_type IN ('pass', 'run')
    GROUP BY defteam
)
SELECT 
    p.passer_player_name,
    CASE 
        WHEN d.def_rank <= 8 THEN 'Top 8 Defense'
        WHEN d.def_rank <= 16 THEN 'Middle Defense'
        ELSE 'Bottom Defense'
    END as defense_tier,
    COUNT(*) as attempts,
    ROUND(100.0 * SUM(CASE WHEN p.complete_pass = true THEN 1 ELSE 0 END) / COUNT(*), 1) as comp_pct,
    SUM(p.passing_yards) as total_yards,
    SUM(p.pass_touchdown) as touchdowns,
    ROUND(AVG(p.epa), 3) as avg_epa
FROM bronze.nfl_play_by_play p
JOIN defense_rankings d ON p.defteam = d.defteam
WHERE p.pass_attempt = true
  AND p.season = 2023
  AND p.passer_player_name IS NOT NULL
GROUP BY p.passer_player_name, defense_tier
HAVING COUNT(*) >= 30
ORDER BY p.passer_player_name, defense_tier;


-- PRACTICE CHALLENGES:
-- 1. Find QBs with the best performance when trailing by 7+ points
-- 2. Analyze QB performance in no-huddle vs regular offense
-- 3. Compare QB stats in home vs away games
-- 4. Find which QBs improve most from 1st half to 2nd half
-- 5. Analyze QB performance based on time remaining in game