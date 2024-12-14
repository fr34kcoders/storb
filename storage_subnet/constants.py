from enum import StrEnum

MIN_PIECE_SIZE = 16 * 1024  # 16 KiB
MAX_PIECE_SIZE = 16 * 1024 * 1024  # 16 MiB
PIECE_LENGTH_SCALING = 0.5
PIECE_LENGTH_OFFSET = 8.39  # these params should split a 1GiB file to 128 pieces

MAX_UPLOAD_SIZE = 1 * 1024 * 1024 * 1024 * 1024  # 1 TiB

# Error correction encoding parameters
# For every 4 data pieces, there are 2 parity pieces
EC_DATA_SIZE = 4
EC_PARITY_SIZE = 2


class LogColor(StrEnum):
    RESET = "\033[0m"
    GREEN = "\033[32m"
    BLUE = "\033[34m"
    BOLD = "\033[1m"
