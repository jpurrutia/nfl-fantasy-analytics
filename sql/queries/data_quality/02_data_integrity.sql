-- Data Integrity and Relationship Checks
-- Run these to identify orphaned records, inconsistencies, and data quality issues

-- =============================================================================
-- ORPHANED RECORDS
-- =============================================================================

-- Performance records without matching players (should be 0)
SELECT 
    'Orphaned Performance Records' as check_name,
    COUNT(*) as count,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM bronze.player_performance p
WHERE NOT EXISTS (
    SELECT 1 FROM bronze.players pl 
    WHERE pl.player_id = p.player_id
);

-- Player mappings with invalid universal_player_id (should be 0)
SELECT 
    'Invalid Mapping References' as check_name,
    COUNT(*) as count,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM bronze.player_mapping pm
WHERE NOT EXISTS (
    SELECT 1 FROM bronze.players p 
    WHERE p.player_id = pm.universal_player_id
);

-- =============================================================================
-- DATA REASONABLENESS CHECKS
-- =============================================================================

-- Fantasy points outliers (unusually high values)
SELECT 
    'Fantasy Points > 100' as check_name,
    COUNT(*) as outlier_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.player_performance), 2) as percentage,
    CASE WHEN COUNT(*) < (SELECT COUNT(*) FROM bronze.player_performance) * 0.01 
         THEN '✅ PASS' ELSE '⚠️  REVIEW' END as status
FROM bronze.player_performance
WHERE fantasy_points_ppr > 100;

-- Negative fantasy points (should be rare but possible)
SELECT 
    'Negative Fantasy Points' as check_name,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM bronze.player_performance), 2) as percentage,
    CASE WHEN COUNT(*) < (SELECT COUNT(*) FROM bronze.player_performance) * 0.05 
         THEN '✅ PASS' ELSE '⚠️  REVIEW' END as status
FROM bronze.player_performance
WHERE fantasy_points_ppr < 0;

-- =============================================================================
-- STATISTICAL OUTLIERS
-- =============================================================================

-- Players with unusually high single-game stats
WITH stat_outliers AS (
    SELECT 
        player_id,
        week,
        season,
        passing_yards,
        rushing_yards, 
        receiving_yards,
        fantasy_points_ppr
    FROM bronze.player_performance
    WHERE passing_yards > 500
       OR rushing_yards > 300
       OR receiving_yards > 300
       OR fantasy_points_ppr > 80
)
SELECT 
    'Statistical Outliers' as check_name,
    COUNT(*) as count,
    '📊 INFO' as status
FROM stat_outliers;

-- =============================================================================
-- CONSISTENCY CHECKS
-- =============================================================================

-- Players with inconsistent team assignments within same season
WITH team_changes AS (
    SELECT 
        p.player_id,
        p.name,
        p.season,
        COUNT(DISTINCT p.recent_team) as team_count
    FROM bronze.player_performance p
    JOIN bronze.players pl ON p.player_id = pl.player_id
    GROUP BY p.player_id, p.name, p.season
    HAVING COUNT(DISTINCT p.recent_team) > 1
)
SELECT 
    'Mid-Season Team Changes' as check_name,
    COUNT(*) as player_count,
    '📊 INFO' as status
FROM team_changes;

-- Players in performance table but not in active/reserve status
SELECT 
    'Performance Players Not ACT/RES' as check_name,
    COUNT(DISTINCT p.player_id) as count,
    CASE WHEN COUNT(DISTINCT p.player_id) < 100 
         THEN '✅ PASS' ELSE '⚠️  REVIEW' END as status
FROM bronze.player_performance p
JOIN bronze.players pl ON p.player_id = pl.player_id
WHERE pl.status NOT IN ('ACT', 'RES');