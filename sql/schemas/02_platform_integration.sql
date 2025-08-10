-- Phase 0: Platform Integration Layer Tables
-- These tables handle the connection between different fantasy platforms

-- User leagues from ESPN/Yahoo/etc
CREATE TABLE IF NOT EXISTS bronze.user_leagues (
    league_id VARCHAR PRIMARY KEY,
    platform VARCHAR NOT NULL,  -- 'ESPN', 'Yahoo', 'DraftKings', etc.
    user_id VARCHAR,
    league_name VARCHAR,
    season INTEGER NOT NULL,
    scoring_settings JSON,  -- Store platform-specific scoring rules
    roster_requirements JSON,  -- Store roster position requirements
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Player mapping between platforms
CREATE TABLE IF NOT EXISTS bronze.player_mapping (
    universal_player_id VARCHAR PRIMARY KEY,
    platform VARCHAR NOT NULL,
    platform_player_id VARCHAR NOT NULL,
    player_name VARCHAR NOT NULL,
    player_name_variant VARCHAR,  -- Alternative names/spellings
    position VARCHAR,
    team VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(platform, platform_player_id)
);