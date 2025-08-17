-- ============================================================================
-- Silver Layer Transformation: Player Opportunity
-- This will become a dbt model: models/silver/player_opportunity.sql
-- ============================================================================
-- Purpose: Combine raw data from multiple bronze sources to create 
--          comprehensive player opportunity metrics
-- ============================================================================

WITH 
-- Step 1: Get snap count data
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
    WHERE offense_snaps > 0  -- Only offensive players
),

-- Step 2: Calculate targets and receptions from play-by-play
target_data AS (
    SELECT 
        receiver_player_name as player,
        season,
        week,
        posteam as team,
        COUNT(*) as targets,
        SUM(CASE WHEN complete_pass = true THEN 1 ELSE 0 END) as receptions,
        SUM(air_yards) as total_air_yards
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true 
      AND receiver_player_name IS NOT NULL
    GROUP BY receiver_player_name, season, week, posteam
),

-- Step 3: Calculate team-level target totals for share calculations
team_targets AS (
    SELECT 
        posteam as team,
        season,
        week,
        COUNT(*) as team_total_targets,
        SUM(air_yards) as team_total_air_yards
    FROM bronze.nfl_play_by_play
    WHERE pass_attempt = true 
      AND receiver_player_name IS NOT NULL
    GROUP BY posteam, season, week
),

-- Step 4: Calculate carries from play-by-play
carry_data AS (
    SELECT 
        rusher_player_name as player,
        season,
        week,
        posteam as team,
        COUNT(*) as carries,
        SUM(rushing_yards) as rushing_yards
    FROM bronze.nfl_play_by_play
    WHERE rush_attempt = true 
      AND rusher_player_name IS NOT NULL
    GROUP BY rusher_player_name, season, week, posteam
),

-- Step 5: Calculate team-level carry totals
team_carries AS (
    SELECT 
        posteam as team,
        season,
        week,
        COUNT(*) as team_total_carries
    FROM bronze.nfl_play_by_play
    WHERE rush_attempt = true 
      AND rusher_player_name IS NOT NULL
    GROUP BY posteam, season, week
),

-- Step 6: Calculate red zone opportunities
rz_data AS (
    SELECT 
        COALESCE(receiver_player_name, rusher_player_name) as player,
        season,
        week,
        posteam as team,
        SUM(CASE 
            WHEN pass_attempt = true AND yardline_100 <= 20 
                 AND receiver_player_name IS NOT NULL 
            THEN 1 ELSE 0 
        END) as rz_targets,
        SUM(CASE 
            WHEN rush_attempt = true AND yardline_100 <= 20 
                 AND rusher_player_name IS NOT NULL 
            THEN 1 ELSE 0 
        END) as rz_carries,
        SUM(CASE 
            WHEN yardline_100 <= 20 
                 AND (receiver_player_name IS NOT NULL OR rusher_player_name IS NOT NULL)
            THEN 1 ELSE 0 
        END) as rz_touches
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
    GROUP BY COALESCE(receiver_player_name, rusher_player_name), season, week, posteam
),

-- Step 7: Calculate team red zone totals
team_rz AS (
    SELECT 
        posteam as team,
        season,
        week,
        COUNT(*) as team_rz_plays
    FROM bronze.nfl_play_by_play
    WHERE yardline_100 <= 20
      AND (pass_attempt = true OR rush_attempt = true)
    GROUP BY posteam, season, week
),

-- Step 8: Get QB-specific metrics from NGS passing
qb_metrics AS (
    SELECT 
        player_display_name as player,
        season,
        week,
        team_abbr as team,
        attempts as pass_attempts,
        avg_time_to_throw as time_to_throw,
        aggressiveness as aggressiveness_score,
        completion_percentage_above_expectation as cpoe
    FROM bronze.nfl_ngs_passing
),

-- Step 9: Get RB-specific metrics from NGS rushing
rb_metrics AS (
    SELECT 
        player_display_name as player,
        season,
        week,
        team_abbr as team,
        percent_attempts_gte_eight_defenders as stacked_box_rate,
        100.0 - percent_attempts_gte_eight_defenders as light_box_rate,
        rush_yards_over_expected as yards_over_expected,
        rush_yards_over_expected_per_att as yoe_per_attempt
    FROM bronze.nfl_ngs_rushing
),

-- Step 10: Get WR/TE-specific metrics from NGS receiving
wr_metrics AS (
    SELECT 
        player_display_name as player,
        season,
        week,
        team_abbr as team,
        avg_separation,
        avg_cushion,
        avg_yac_above_expectation as yac_above_expected,
        percent_share_of_intended_air_yards as air_yards_share_ngs
    FROM bronze.nfl_ngs_receiving
),

-- Step 11: Combine all data sources
combined_data AS (
    SELECT 
        -- Player identification
        COALESCE(s.player, t.player, c.player, rz.player) as player_id,
        COALESCE(s.season, t.season, c.season, rz.season) as season,
        COALESCE(s.week, t.week, c.week, rz.week) as week,
        COALESCE(s.team, t.team, c.team, rz.team) as team,
        s.position,
        
        -- Snap metrics
        s.snap_count,
        s.snap_pct,
        
        -- Usage metrics
        COALESCE(c.carries, 0) + COALESCE(t.receptions, 0) as touches,
        COALESCE(c.rushing_yards, 0) + COALESCE(t.receptions * 10, 0) as total_yards, -- Simplified
        
        -- Target metrics
        COALESCE(t.targets, 0) as targets,
        CASE 
            WHEN tt.team_total_targets > 0 
            THEN ROUND(100.0 * t.targets / tt.team_total_targets, 2)
            ELSE 0 
        END as target_share,
        COALESCE(t.receptions, 0) as receptions,
        
        -- Carry metrics
        COALESCE(c.carries, 0) as carries,
        CASE 
            WHEN tc.team_total_carries > 0 
            THEN ROUND(100.0 * c.carries / tc.team_total_carries, 2)
            ELSE 0 
        END as carry_share,
        
        -- Red zone metrics
        COALESCE(rz.rz_touches, 0) as rz_touches,
        CASE 
            WHEN trz.team_rz_plays > 0 
            THEN ROUND(100.0 * rz.rz_touches / trz.team_rz_plays, 2)
            ELSE 0 
        END as rz_share,
        COALESCE(rz.rz_targets, 0) as rz_targets,
        COALESCE(rz.rz_carries, 0) as rz_carries,
        
        -- Air yards metrics
        COALESCE(t.total_air_yards, 0) as air_yards,
        CASE 
            WHEN tt.team_total_air_yards > 0 
            THEN ROUND(100.0 * t.total_air_yards / tt.team_total_air_yards, 2)
            ELSE 0 
        END as air_yards_share,
        
        -- Route metrics (placeholder - needs more complex calculation)
        NULL::INTEGER as routes_run,
        NULL::DECIMAL(5,2) as route_participation,
        
        -- Position-specific metrics
        qb.pass_attempts,
        NULL::INTEGER as dropbacks,  -- Would need to calculate from play-by-play
        qb.time_to_throw,
        qb.aggressiveness_score,
        
        rb.stacked_box_rate,
        rb.light_box_rate,
        rb.yards_over_expected,
        
        wr.avg_separation,
        wr.avg_cushion,
        wr.yac_above_expected,
        
        -- Metadata
        CONCAT_WS(',',
            CASE WHEN s.snap_count IS NOT NULL THEN 'snaps' END,
            CASE WHEN t.targets IS NOT NULL THEN 'targets' END,
            CASE WHEN c.carries IS NOT NULL THEN 'carries' END,
            CASE WHEN rz.rz_touches IS NOT NULL THEN 'redzone' END,
            CASE WHEN qb.pass_attempts IS NOT NULL THEN 'ngs_passing' END,
            CASE WHEN rb.yards_over_expected IS NOT NULL THEN 'ngs_rushing' END,
            CASE WHEN wr.avg_separation IS NOT NULL THEN 'ngs_receiving' END
        ) as data_sources,
        
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
        
    FROM snap_data s
    FULL OUTER JOIN target_data t 
        ON s.player = t.player AND s.season = t.season AND s.week = t.week
    FULL OUTER JOIN carry_data c
        ON s.player = c.player AND s.season = c.season AND s.week = c.week
    FULL OUTER JOIN rz_data rz
        ON s.player = rz.player AND s.season = rz.season AND s.week = rz.week
    LEFT JOIN team_targets tt
        ON COALESCE(s.team, t.team) = tt.team 
        AND COALESCE(s.season, t.season) = tt.season 
        AND COALESCE(s.week, t.week) = tt.week
    LEFT JOIN team_carries tc
        ON COALESCE(s.team, c.team) = tc.team 
        AND COALESCE(s.season, c.season) = tc.season 
        AND COALESCE(s.week, c.week) = tc.week
    LEFT JOIN team_rz trz
        ON COALESCE(s.team, rz.team) = trz.team 
        AND COALESCE(s.season, rz.season) = trz.season 
        AND COALESCE(s.week, rz.week) = trz.week
    LEFT JOIN qb_metrics qb
        ON s.player = qb.player AND s.season = qb.season AND s.week = qb.week
    LEFT JOIN rb_metrics rb
        ON s.player = rb.player AND s.season = rb.season AND s.week = rb.week
    LEFT JOIN wr_metrics wr
        ON s.player = wr.player AND s.season = wr.season AND s.week = wr.week
)

-- Final output
SELECT * FROM combined_data
WHERE player_id IS NOT NULL
  AND season IS NOT NULL
  AND week IS NOT NULL;

-- ============================================================================
-- dbt Configuration (when we migrate to dbt)
-- ============================================================================
-- {{ config(
--     materialized = 'incremental',
--     unique_key = ['player_id', 'season', 'week'],
--     on_schema_change = 'fail',
--     indexes = [
--         {'columns': ['player_id'], 'type': 'btree'},
--         {'columns': ['season', 'week'], 'type': 'btree'},
--         {'columns': ['position'], 'type': 'btree'},
--         {'columns': ['team'], 'type': 'btree'}
--     ]
-- ) }}

-- For incremental runs in dbt:
-- {% if is_incremental() %}
--   WHERE season = {{ var('season') }}
--     AND week = {{ var('week') }}
-- {% endif %}