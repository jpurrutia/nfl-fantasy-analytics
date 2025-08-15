-- Migration 001: Document Initial Schema State
-- This migration documents the initial database schema as of Phase 1 completion
-- It doesn't create tables (they already exist), but documents the starting point

-- Note: This is a documentation migration - tables already exist
-- Future migrations will build on this baseline

-- Bronze Layer Tables (as of Phase 1 completion)
-- Table: bronze.players
-- Description: NFL player roster data from nfl-data-py
-- Primary Key: player_id

-- Table: bronze.player_performance  
-- Description: Weekly player performance statistics from nfl-data-py
-- Primary Key: (player_id, season, week)

-- Table: bronze.player_opportunity
-- Description: Player opportunity metrics (snap counts, targets, etc.)
-- Primary Key: (player_id, season, week)

-- Table: bronze.player_mapping
-- Description: Cross-platform player ID mapping (ESPN ↔ NFL)
-- Primary Key: universal_player_id

-- Table: bronze.user_leagues
-- Description: ESPN fantasy league metadata
-- Primary Key: (league_id, platform)

-- Silver and Gold schemas exist but have no tables yet
-- Schema: silver (for cleaned, standardized data)
-- Schema: gold (for business-ready analytics)

-- Migration tracking table
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    description VARCHAR,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    checksum VARCHAR  -- For verifying migration integrity
);