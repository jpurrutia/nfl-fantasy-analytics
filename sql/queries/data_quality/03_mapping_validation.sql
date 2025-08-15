-- Player Mapping Validation Queries
-- Validate the quality and completeness of ESPN <-> NFL player mappings

-- =============================================================================
-- MAPPING COVERAGE AND DISTRIBUTION
-- =============================================================================

-- Platform distribution in mappings
SELECT 
    platform,
    COUNT(*) as mapping_count,
    COUNT(DISTINCT universal_player_id) as unique_players,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
FROM bronze.player_mapping
GROUP BY platform
ORDER BY mapping_count DESC;

-- =============================================================================
-- MAPPING QUALITY CHECKS
-- =============================================================================

-- Check for duplicate ESPN player mappings (should be unique)
WITH espn_duplicates AS (
    SELECT 
        platform_player_id,
        COUNT(*) as mapping_count
    FROM bronze.player_mapping
    WHERE platform = 'ESPN'
    GROUP BY platform_player_id
    HAVING COUNT(*) > 1
)
SELECT 
    'Duplicate ESPN Mappings' as check_name,
    COUNT(*) as duplicate_count,
    CASE WHEN COUNT(*) = 0 THEN '✅ PASS' ELSE '❌ FAIL' END as status
FROM espn_duplicates;

-- Mappings where player names are very different (potential mapping errors)
WITH name_mismatches AS (
    SELECT 
        pm.platform_player_id,
        pm.player_name as espn_name,
        pm.player_name_variant as nfl_name,
        p.name as actual_nfl_name,
        p.position,
        p.team
    FROM bronze.player_mapping pm
    JOIN bronze.players p ON pm.universal_player_id = p.player_id
    WHERE pm.platform = 'ESPN'
      AND LOWER(pm.player_name) != LOWER(pm.player_name_variant)
      -- Flag cases where names are very different (less than 50% similarity would be ideal)
      AND LENGTH(pm.player_name) > 0
      AND LENGTH(pm.player_name_variant) > 0
)
SELECT 
    'Potential Name Mismatches' as check_name,
    COUNT(*) as count,
    '⚠️  REVIEW' as status
FROM name_mismatches;

-- =============================================================================
-- POSITION AND TEAM CONSISTENCY
-- =============================================================================

-- Check position consistency between mapping and player tables
WITH position_mismatches AS (
    SELECT 
        pm.platform_player_id,
        pm.player_name,
        pm.position as mapping_position,
        p.position as player_position
    FROM bronze.player_mapping pm
    JOIN bronze.players p ON pm.universal_player_id = p.player_id
    WHERE pm.platform = 'ESPN'
      AND pm.position != p.position
      -- Allow some common variations
      AND NOT (pm.position = 'RB' AND p.position = 'FB')
      AND NOT (pm.position = 'WR' AND p.position = 'TE')
)
SELECT 
    'Position Mismatches' as check_name,
    COUNT(*) as mismatch_count,
    CASE WHEN COUNT(*) < 5 THEN '✅ PASS' ELSE '⚠️  REVIEW' END as status
FROM position_mismatches;

-- =============================================================================
-- SAMPLE MAPPING VALIDATION
-- =============================================================================

-- Show a sample of mappings for manual verification
SELECT 
    'Sample Mappings' as section,
    pm.player_name as espn_name,
    pm.player_name_variant as nfl_name,
    pm.position,
    pm.team,
    p.name as actual_nfl_name,
    p.position as actual_position,
    p.team as actual_team,
    p.status
FROM bronze.player_mapping pm
JOIN bronze.players p ON pm.universal_player_id = p.player_id
WHERE pm.platform = 'ESPN'
ORDER BY RANDOM()
LIMIT 10;

-- =============================================================================
-- TOP PLAYERS MAPPING CHECK
-- =============================================================================

-- Verify that top fantasy performers have mappings
WITH top_performers AS (
    SELECT DISTINCT
        pp.player_id,
        p.name,
        p.position,
        p.team,
        SUM(pp.fantasy_points_ppr) as total_fantasy_points
    FROM bronze.player_performance pp
    JOIN bronze.players p ON pp.player_id = p.player_id
    WHERE pp.season = 2024
      AND p.position IN ('QB', 'RB', 'WR', 'TE')
    GROUP BY pp.player_id, p.name, p.position, p.team
    ORDER BY total_fantasy_points DESC
    LIMIT 50
)
SELECT 
    'Top 50 Players Mapped' as check_name,
    COUNT(*) as mapped_count,
    '50' as total_top_players,
    ROUND(COUNT(*) * 100.0 / 50, 1) as mapping_percentage,
    CASE WHEN COUNT(*) >= 40 THEN '✅ PASS' ELSE '⚠️  REVIEW' END as status
FROM top_performers tp
WHERE EXISTS (
    SELECT 1 FROM bronze.player_mapping pm 
    WHERE pm.universal_player_id = tp.player_id 
    AND pm.platform = 'ESPN'
);