-- Create schema layers for medallion architecture
-- Bronze: Raw data as ingested
-- Silver: Cleaned and standardized data
-- Gold: Analytics-ready aggregated data

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;