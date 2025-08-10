-- Phase 1: Core Fantasy Model Tables
-- These tables store the fundamental player and performance data

-- Players master table
CREATE TABLE IF NOT EXISTS bronze.players (
    player_id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    position VARCHAR NOT NULL,
    team VARCHAR,
    status VARCHAR DEFAULT 'active',  -- active, injured, IR, out
    birth_date DATE,
    college VARCHAR,
    draft_year INTEGER,
    draft_round INTEGER,
    draft_pick INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Player performance by week
CREATE TABLE IF NOT EXISTS bronze.player_performance (
    player_id VARCHAR NOT NULL,
    week INTEGER NOT NULL,
    season INTEGER NOT NULL,
    game_date DATE,
    opponent VARCHAR,
    -- Passing stats
    passing_attempts INTEGER,
    passing_completions INTEGER,
    passing_yards INTEGER,
    passing_tds INTEGER,
    passing_ints INTEGER,
    -- Rushing stats
    rushing_attempts INTEGER,
    rushing_yards INTEGER,
    rushing_tds INTEGER,
    -- Receiving stats
    targets INTEGER,
    receptions INTEGER,
    receiving_yards INTEGER,
    receiving_tds INTEGER,
    -- Misc stats
    fumbles_lost INTEGER,
    two_point_conversions INTEGER,
    -- Fantasy points
    fantasy_points_standard DECIMAL(10,2),
    fantasy_points_ppr DECIMAL(10,2),
    fantasy_points_half_ppr DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season, week),
    FOREIGN KEY (player_id) REFERENCES bronze.players(player_id)
);

-- Player opportunity metrics
CREATE TABLE IF NOT EXISTS bronze.player_opportunity (
    player_id VARCHAR NOT NULL,
    week INTEGER NOT NULL,
    season INTEGER NOT NULL,
    -- Snap data
    snap_count INTEGER,
    snap_pct DECIMAL(5,2),
    -- Target/carry data
    targets INTEGER,
    target_share DECIMAL(5,2),
    carries INTEGER,
    carry_share DECIMAL(5,2),
    -- Red zone data
    rz_touches INTEGER,
    rz_share DECIMAL(5,2),
    rz_targets INTEGER,
    rz_carries INTEGER,
    -- Route data
    routes_run INTEGER,
    route_participation DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (player_id, season, week),
    FOREIGN KEY (player_id) REFERENCES bronze.players(player_id)
);