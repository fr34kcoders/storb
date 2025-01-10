from enum import StrEnum

# TODO: Make this variable dependent on file/piece size?
QUERY_TIMEOUT = 3
NUM_UIDS_QUERY = 10  # Default number of uids to query per store request
QUERY_RATE = 10  # In blocks
MAX_QUERY_BATCH_SIZE = 20  # Default max query batch size
PROCESS_COUNT = 10  # Number of processes to spawn for parallel processing

MIN_PIECE_SIZE = 16 * 1024  # 16 KiB
MAX_PIECE_SIZE = 256 * 1024 * 1024  # 256 MiB
PIECE_LENGTH_SCALING = 0.5
PIECE_LENGTH_OFFSET = 8.39

MAX_UPLOAD_SIZE = 1 * 1024 * 1024 * 1024 * 1024  # 1 TiB

# Error correction encoding parameters
# For every 4 data pieces, there are 2 parity pieces
EC_DATA_SIZE = 4
EC_PARITY_SIZE = 2

DHT_QUERY_TIMEOUT = 10
DHT_STARTUP_AND_SHUTDOWN_TIMEOUT = 5

# Default object store dir
STORE_DIR = "object_store"

DEFAULT_RSA_KEY_SIZE = 2048
G_CANDIDATE_RETRY = (
    1000  # Number of times to retry g (RSA Generator) candidate generation
)
S_CANDIDATE_RETRY = 1000  # Number of times to retry s (RSA Secret) candidate generation


class NeuronType(StrEnum):
    Base = "base"
    Miner = "miner"
    Validator = "validator"
