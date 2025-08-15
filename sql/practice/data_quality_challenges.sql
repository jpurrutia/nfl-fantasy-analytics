-- NFL Analytics Data Quality Challenges 📊
-- LeetCode-style SQL problems using your real NFL data!
-- Each challenge builds practical data validation skills

-- =============================================================================
-- CHALLENGE 1: Basic Data Audit (Easy) 
-- =============================================================================
-- Problem: Write a query to count total records in each bronze table and identify
-- which table has the most records.
--
-- Expected output columns: table_name, record_count, rank
-- Order by: record_count DESC

/* Your solution here:

SELECT 
    -- Fill this in
FROM 
    -- Fill this in

*/

-- =============================================================================
-- CHALLENGE 2: Missing Data Detective (Easy-Medium)
-- =============================================================================
-- Problem: Find all players who have performance records but are missing 
-- critical player information (name, position, or team is NULL).
--
-- Expected output: player_id, missing_field, status
-- Show what field is missing for each problematic player

/* Your solution here:

*/

-- =============================================================================
-- CHALLENGE 3: Fantasy Points Outlier Analysis (Medium)
-- =============================================================================
-- Problem: Identify the top 5 single-game fantasy performances that might be 
-- data entry errors (>75 points) and show the player details.
--
-- Expected output: player_name, position, team, week, season, fantasy_points_ppr, 
--                  avg_season_points, points_vs_avg_ratio
-- Calculate how many times higher than their season average this game was

/* Your solution here:

*/

-- =============================================================================
-- CHALLENGE 4: Data Consistency Check (Medium)
-- =============================================================================
-- Problem: Find players who appear to have played for multiple teams in the 
-- same season based on their performance records, but show only cases where
-- this might be a data error (not legitimate trades).
--
-- Hint: Look for cases where a player has >2 different teams in performance
-- data but their main player record shows only one team
--
-- Expected output: player_name, season, teams_in_performance, teams_in_roster

/* Your solution here:

*/

-- =============================================================================
-- CHALLENGE 5: Mapping Quality Assessment (Hard)
-- =============================================================================
-- Problem: Create a comprehensive mapping quality report showing:
-- 1. What percentage of top 100 fantasy performers have ESPN mappings
-- 2. How many high-value players are missing mappings
-- 3. Position breakdown of missing mappings
--
-- Expected output: position, top_performers_count, mapped_count, missing_count, 
--                  mapping_percentage
-- Only include players with >100 total fantasy points in 2024

/* Your solution here:

*/

-- =============================================================================
-- CHALLENGE 6: Weekly Data Completeness (Hard)
-- =============================================================================
-- Problem: Create a "data quality dashboard" showing week-by-week completeness.
-- For each week in 2024, calculate:
-- - Total player records
-- - Records with non-zero fantasy points 
-- - Records with complete stat lines (passing/rushing/receiving not all null)
-- - Data quality percentage
--
-- Expected output: week, total_records, active_records, complete_records, 
--                  quality_percentage
-- Flag weeks with <90% quality as "REVIEW NEEDED"

/* Your solution here:

*/

-- =============================================================================
-- CHALLENGE 7: Cross-Reference Validation (Expert)
-- =============================================================================
-- Problem: Build a "trust score" for each player's data by checking consistency
-- across multiple dimensions:
-- 1. Position consistency (mapping vs roster)
-- 2. Team consistency (performance vs roster) 
-- 3. Name consistency (mapping names match)
-- 4. Statistical reasonableness (fantasy points align with raw stats)
--
-- Create a score from 0-100 where 100 is perfect data quality.
-- Show the 10 players with the lowest trust scores who have >50 fantasy points.
--
-- Expected output: player_name, position, team, total_fantasy_points, trust_score,
--                  issue_summary

/* Your solution here:

*/

-- =============================================================================
-- 💡 BONUS CHALLENGE: Data Quality Monitoring
-- =============================================================================
-- Problem: Write a query that could be run daily to catch new data quality issues.
-- Create a single query that returns a dashboard with key metrics:
-- - Total records added today (if we had timestamps)
-- - Any new orphaned records
-- - Any new statistical outliers
-- - Current mapping coverage percentage
-- - Overall health score (your own formula)

/* Your solution here:

*/

-- =============================================================================
-- 📚 HINTS AND TIPS
-- =============================================================================

-- Hint for Challenge 1:
-- Use UNION ALL to combine counts from different tables
-- Use ROW_NUMBER() OVER (ORDER BY count DESC) for ranking

-- Hint for Challenge 2: 
-- Use CASE statements to identify which field is NULL
-- Consider using UNION ALL to check multiple conditions

-- Hint for Challenge 3:
-- Use window functions: AVG() OVER (PARTITION BY player_id)
-- Join player_performance with players table for names

-- Hint for Challenge 4:
-- Compare COUNT(DISTINCT recent_team) in performance vs team in players
-- Use HAVING clause to filter for multi-team cases

-- Hint for Challenge 5:
-- Use EXISTS() to check if mapping exists
-- Calculate percentages with ROUND(COUNT(*) * 100.0 / total, 1)

-- Hint for Challenge 6:
-- Use conditional aggregation: SUM(CASE WHEN condition THEN 1 ELSE 0 END)
-- Window functions can help calculate percentages

-- Hint for Challenge 7:
-- Create multiple scoring components, then combine them
-- Use COALESCE() to handle NULLs in calculations
-- Consider using CASE statements for weighted scoring

-- =============================================================================
-- 🎯 LEARNING OBJECTIVES
-- =============================================================================
-- After completing these challenges, you'll be able to:
-- ✓ Audit data completeness and quality
-- ✓ Identify statistical outliers and anomalies
-- ✓ Validate cross-table relationships
-- ✓ Build automated data quality monitoring
-- ✓ Create actionable data quality reports
-- ✓ Use advanced SQL for data validation

-- Try solving these step by step. Each builds on the previous ones!
-- Check your solutions by running them against the actual data.