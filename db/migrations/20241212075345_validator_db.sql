-- migrate:up

-- Table for mapping infohash to file metadata and chunk IDs
CREATE TABLE tracker (
    infohash TEXT PRIMARY KEY, -- Infohash
    validator_id INT,           -- ID of the validator
    filename TEXT,              -- Name of the file
    length INTEGER,             -- Length of the file in bytes
    chunk_length INTEGER,       -- Length of each chunk in bytes
    chunk_count INTEGER         -- Number of chunks in the file
    chunk_hashes TEXT           -- JSON Array of chunk hashes
    creation_time TIMESTAMP     -- Timestamp of creation
    signature TEXT              -- Signature of the DHT entry by the validator
);

-- Table for mapping chunk_hash to chunk metadata and piece hashes
CREATE TABLE chunk (
    chunk_hash TEXT PRIMARY KEY, -- Chunk ID
    infohash TEXT,               -- Infohash of the file the chunk belongs to
    validator_id INT,            -- ID of the validator
    piece_hashes TEXT            -- JSON Array of piece hashes
    chunk_idx INTEGER,           -- Index of the chunk in the file
    k INTEGER,                   -- Number data pieces in the chunk
    m INTEGER,                   -- Number of total pieces in the chunk
    chunk_size INTEGER,          -- Size of the chunk in bytes as zfec calculations
    padlen INTEGER,              -- Padding length
    original_chunk_size INTEGER, -- Size of the chunk in bytes before padding
    signature TEXT               -- Signature of the DHT entry by the validator
)

-- Table for mapping piece_hash to piece metadata and miner_id
CREATE TABLE piece (
    piece_hash TEXT PRIMARY KEY, -- Piece ID
    chunk_hash TEXT,             -- Chunk ID of the piece belongs to
    miner_id INTEGER,            -- ID of the miner
    chunk_idx INTEGER,           -- Index of the chunk in the file
    piece_idx INTEGER,           -- Index of the piece in the chunk
    piece_type INTEGER CHECK (piece_type IN (0, 1)),          -- Type of the piece (0: data, 1: parity)
    signature TEXT               -- Signature of the DHT entry by the miner storing the piece
)


-- Table for miner stats --
CREATE TABLE miner_stats (
    miner_uid INTEGER PRIMARY KEY,
    challenge_successes INTEGER,
    challenge_attempts INTEGER,
    retrieval_successes INTEGER,
    retrieval_attempts INTEGER,
    store_successes INTEGER,
    store_attempts INTEGER,
    total_successes INTEGER
)

-- migrate:down

-- Drop the miner_stats table
DROP TABLE IF EXISTS miner_stats;

-- Drop the Tracker table
DROP TABLE IF EXISTS Tracker;

-- Drop the Chunk table
DROP TABLE IF EXISTS Chunk;

-- Drop the Piece table
DROP TABLE IF EXISTS Piece;
