-- migrate:up

-- Table for mapping infohash to piece IDs
CREATE TABLE infohash_piece_ids (
    infohash TEXT PRIMARY KEY, -- Primary key for the table
    piece_ids TEXT             -- JSON string representing piece IDs
);

-- Table for metadata associated with files
CREATE TABLE metadata (
    filename TEXT,             -- Name of the file
    timestamp TEXT,            -- Timestamp as a string (e.g., ISO 8601 format)
    piece_length INTEGER,      -- Length of each piece in bytes
    length INTEGER             -- Total length of the file in bytes
);

-- -- Table for tracking infohash and validator UID in the DHT
-- CREATE TABLE tracker_dht (
--     infohash TEXT PRIMARY KEY, -- Primary key for the table
--     validator_uid TEXT         -- UID of the validator
-- );

-- Table for mapping piece IDs to miner UIDs
CREATE TABLE piece_id_miner_uids (
    piece_id TEXT PRIMARY KEY, -- Primary key for the table
    miner_uids TEXT            -- JSON string representing miner UIDs
);

-- migrate:down
-- Drop the table mapping piece IDs to miner UIDs
DROP TABLE IF EXISTS piece_id_miner_uids;

-- Drop the table for tracking infohash and validator UID in the DHT
-- DROP TABLE IF EXISTS tracker_dht;

-- Drop the table for metadata associated with files
DROP TABLE IF EXISTS metadata;

-- Drop the table mapping infohash to piece IDs
DROP TABLE IF EXISTS infohash_piece_ids;
