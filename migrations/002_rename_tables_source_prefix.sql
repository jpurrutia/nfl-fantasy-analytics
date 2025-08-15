-- Migration 002: Rename Tables with Source Prefixes
-- Create new tables with source prefixes and copy data
CREATE TABLE bronze.nfl_players AS SELECT * FROM bronze.players;
CREATE TABLE bronze.nfl_player_performance AS SELECT * FROM bronze.player_performance;
CREATE TABLE bronze.nfl_player_opportunity AS SELECT * FROM bronze.player_opportunity;
CREATE TABLE bronze.player_id_mapping AS SELECT * FROM bronze.player_mapping;
-- Backup old tables by renaming them
CREATE TABLE bronze.players_backup AS SELECT * FROM bronze.players;
CREATE TABLE bronze.player_performance_backup AS SELECT * FROM bronze.player_performance;
CREATE TABLE bronze.player_opportunity_backup AS SELECT * FROM bronze.player_opportunity;
CREATE TABLE bronze.player_mapping_backup AS SELECT * FROM bronze.player_mapping;
-- Drop original tables
DROP TABLE bronze.players;
DROP TABLE bronze.player_performance;
DROP TABLE bronze.player_opportunity;
DROP TABLE bronze.player_mapping;