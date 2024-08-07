-- Use the play_by_play schema
create Schema if not EXISTS 'play_by_play';
SET SCHEMA 'play_by_play';

-- Drop tables if they already exist
DROP TABLE IF EXISTS play_by_play;
DROP TABLE IF EXISTS depth_charts;
DROP TABLE IF EXISTS injuries;
DROP TABLE IF EXISTS qbr;
DROP TABLE IF EXISTS ids_data;
DROP TABLE IF EXISTS team_descriptions;
DROP TABLE IF EXISTS ftn_data;

-- Create tables using read_csv
CREATE TABLE play_by_play AS
SELECT * FROM read_csv('{path}/play_by_play/*.csv');

CREATE TABLE depth_charts AS
SELECT * FROM read_csv('{path}/depth_charts/*.csv');

CREATE TABLE injuries AS
SELECT * FROM read_csv('{path}/injuries/*.csv');

CREATE TABLE qbr AS
SELECT * FROM read_csv('{path}/qbr/*.csv');

CREATE TABLE ids_data AS
SELECT * FROM read_csv('{path}/ids_data/*.csv');

CREATE TABLE team_descriptions AS
SELECT * FROM read_csv('{path}/team_descriptions/*.csv');

CREATE TABLE ftn_data AS
SELECT * FROM read_csv('{path}/ftn_data/*.csv');

-- Indexes
-- Index on 'play_by_play' table
CREATE INDEX idx_play_by_play_team ON play_by_play (team);
CREATE INDEX idx_play_by_play_yardline ON play_by_play (yardline);
CREATE INDEX idx_play_by_play_play_result ON play_by_play (play_result);

-- Index on 'depth_charts' table
CREATE INDEX idx_depth_charts_position ON depth_charts (position);
CREATE INDEX idx_depth_charts_depth ON depth_charts (depth);

-- Index on 'injuries' table
CREATE INDEX idx_injuries_team ON injuries (team);
CREATE INDEX idx_injuries_injury_type ON injuries (injury_type);

-- Index on 'qbr' table
CREATE INDEX idx_qbr_team ON qbr (team);
CREATE INDEX idx_qbr_season ON qbr (season);
CREATE INDEX idx_qbr_week ON qbr (week);

-- Index on 'ids_data' table
CREATE INDEX idx_ids_data_team ON ids_data (team);
CREATE INDEX idx_ids_data_position ON ids_data (position);

-- Index on 'team_descriptions' table
CREATE INDEX idx_team_descriptions_location ON team_descriptions (location);

-- Index on 'ftn_data' table
CREATE INDEX idx_ftn_data_season ON ftn_data (season);
CREATE INDEX idx_ftn_data_week ON ftn_data (week);