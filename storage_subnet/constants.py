from enum import StrEnum

# TODO: make this variable - dependant on file size?
QUERY_TIMEOUT = 10
NUM_UIDS_QUERY = 5
QUERY_RATE = 10  # in blocks
MAX_QUERY_BATCH_SIZE = 100  # default max query batch size

MIN_PIECE_SIZE = 16 * 1024  # 16 KiB
MAX_PIECE_SIZE = 16 * 1024 * 1024  # 16 MiB
PIECE_LENGTH_SCALING = 0.5
PIECE_LENGTH_OFFSET = 8.39  # these params should split a 1GiB file to 128 pieces

MAX_UPLOAD_SIZE = 1 * 1024 * 1024 * 1024 * 1024  # 1 TiB

# Reed-Solomon encoding parameters
# For every 4 data pieces, there are 2 parity pieces
RS_DATA_SIZE = 4
RS_PARITY_SIZE = 2


class LogColor(StrEnum):
    RESET = "\033[0m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    BOLD = "\033[1m"
