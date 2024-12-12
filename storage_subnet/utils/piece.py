import hashlib
import math
from typing import Generator

import reedsolo

from storage_subnet.constants import (
    MAX_PIECE_SIZE,
    MIN_PIECE_SIZE,
    PIECE_LENGTH_OFFSET,
    PIECE_LENGTH_SCALING,
    RS_DATA_SIZE,
    RS_PARITY_SIZE,
)


def piece_length(
    content_length: int, min_size: int = MIN_PIECE_SIZE, max_size: int = MAX_PIECE_SIZE
) -> int:
    """Calculate the appropriate piece size based on content length.

    Args:
        content_length (int): The total length of the content.
        min_size (int, optional): The minimum allowed piece size. Defaults to MIN_PIECE_SIZE.
        max_size (int, optional): The maximum allowed piece size. Defaults to MAX_PIECE_SIZE.

    Returns:
        int: The calculated piece size within the specified bounds.
    """
    exponent = int(
        (math.log2(content_length) * PIECE_LENGTH_SCALING) + PIECE_LENGTH_OFFSET
    )
    length = 1 << exponent
    if length < min_size:
        return min_size
    elif length > max_size:
        return max_size
    return length


def piece_hash(data: bytes) -> str:
    """Calculate the SHA-1 hash of a piece of data.

    Args:
        data (bytes): The data to hash.

    Returns:
        str: The SHA-1 hash of the data.
    """
    return hashlib.sha1(data).hexdigest()


def split_file(
    file_path: str,
    piece_size: int,
    rs_data_pieces: int = RS_DATA_SIZE,
    rs_parity_pieces: int = RS_PARITY_SIZE,
) -> Generator[tuple[str, bytes], None, None]:
    """
    Stream data and parity pieces as they are generated.

    Args:
        file_path (str): Path to the input file.
        piece_size (int): Size of each data piece (in bytes).

    Yields:
        Tuple[str, bytes]: A tuple where the first element indicates "data" or "parity",
                           and the second element is the corresponding piece.
    """
    rs = reedsolo.RSCodec(rs_parity_pieces)

    # Open the file for reading
    with open(file_path, "rb") as f:
        # Initialize buffers for parity pieces
        parity_buffers = [bytearray(piece_size) for _ in range(rs_parity_pieces)]

        # Process the file in chunks
        while True:
            # Read one "row" of bytes across all data pieces
            data_pieces = []
            for _ in range(rs_data_pieces):
                chunk = f.read(piece_size)
                if not chunk:
                    break  # End of file
                # Pad chunk if it's smaller than the piece size
                chunk = chunk.ljust(piece_size, b"\0")
                data_pieces.append(chunk)

            if not data_pieces:
                break

            # Generate parity for the current row of bytes
            for i in range(piece_size):
                row = bytearray(piece[i] for piece in data_pieces)
                parity_row = rs.encode(row)[rs_data_pieces:]  # Get parity bytes
                for j, parity_byte in enumerate(parity_row):
                    parity_buffers[j][i] = parity_byte

            # Yield the current chunks (data pieces)
            for piece in data_pieces:
                yield ("data", piece)

            # Yield the parity pieces for this chunk
            for parity_buffer in parity_buffers:
                yield ("parity", parity_buffer)
