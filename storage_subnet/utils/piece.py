import math

from storage_subnet.constants import MAX_PIECE_SIZE, MIN_PIECE_SIZE


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
    exponent = int((math.log2(content_length) * 0.5) + 4)
    length = 1 << exponent
    if length < min_size:
        return min_size
    elif length > max_size:
        return max_size
    return length


def split_content(file_path: str) -> list[bytes]:
    """Split the content of a file into smaller pieces.

    Args:
        file_path (str): The path to the file to be split.

    Returns:
        list[bytes]: A list of byte chunks representing the file pieces.
    """
    with open(file_path, "rb") as file:
        file.seek(0, 2)  # Move to the end of the file
        content_length = file.tell()
        piece_size = piece_length(content_length)
        file.seek(0)  # Reset to the beginning of the file

        pieces = []
        chunk = file.read(piece_size)
        while chunk:
            pieces.append(chunk)
            chunk = file.read(piece_size)
    return pieces
