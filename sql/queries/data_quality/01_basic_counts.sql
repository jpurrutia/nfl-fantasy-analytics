-- Basic Data Quality Checks - Table Counts and Coverage
-- Run these queries interactively to quickly assess data quality

-- =============================================================================
-- TABLE COUNTS AND BASIC STATS
-- =============================================================================

-- Overall table sizes
SELECT 
    'players' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT player_id) as unique_players
FROM bronze.players

UNION ALL

SELECT 
    'player_performance' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT player_id) as unique_players
FROM bronze.player_performance

UNION ALL

SELECT 
    'player_mapping' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT universal_player_id) as unique_players
FROM bronze.player_mapping;

-- =============================================================================
-- SEASON AND WEEK COVERAGE
-- =============================================================================

-- Performance data coverage by season/week
SELECT 
    season,
    MIN(week) as first_week,
    MAX(week) as last_week,
    COUNT(DISTINCT week) as weeks_with_data,
    COUNT(*) as total_records,
    COUNT(DISTINCT player_id) as unique_players
FROM bronze.player_performance
GROUP BY season
ORDER BY season;

-- =============================================================================
-- POSITION DISTRIBUTION
-- =============================================================================

-- Player counts by position (should have reasonable distributions)
SELECT 
    position,
    COUNT(*) as player_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM bronze.players
WHERE status IN ('ACT', 'RES')  -- Active/Reserve players only
GROUP BY position
ORDER BY player_count DESC;

-- =============================================================================
-- DATA COMPLETENESS CHECKS
-- =============================================================================

-- Check for null values in critical fields
SELECT 
    'players' as table_name,
    'player_id' as field,
    COUNT(*) as total_records,
    SUM(CASE WHEN player_id IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(SUM(CASE WHEN player_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
FROM bronze.players

UNION ALL

SELECT 
    'players', 'name',
    COUNT(*), 
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM bronze.players

UNION ALL

SELECT 
    'players', 'position',
    COUNT(*), 
    SUM(CASE WHEN position IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN position IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM bronze.players

UNION ALL

SELECT 
    'player_performance', 'fantasy_points_ppr',
    COUNT(*), 
    SUM(CASE WHEN fantasy_points_ppr IS NULL THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN fantasy_points_ppr IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
FROM bronze.player_performance;