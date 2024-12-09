import math

from storage_subnet.constants import (
    MAX_PIECE_SIZE,
    MIN_PIECE_SIZE,
    PIECE_LENGTH_OFFSET,
    PIECE_LENGTH_SCALING,
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
