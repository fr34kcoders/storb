import math

import bittensor as bt
from zfec.easyfec import Decoder, Encoder

from storage_subnet.constants import (
    MAX_PIECE_SIZE,
    MIN_PIECE_SIZE,
    PIECE_LENGTH_OFFSET,
    PIECE_LENGTH_SCALING,
)


def piece_length(
    content_length: int, min_size: int = MIN_PIECE_SIZE, max_size: int = MAX_PIECE_SIZE
) -> int:
    """Calculate an approximate piece size based on content length,
    clamped to a [min_size..max_size] range."""
    exponent = int(
        (math.log2(content_length) * PIECE_LENGTH_SCALING) + PIECE_LENGTH_OFFSET
    )
    length = 1 << exponent
    if length < min_size:
        return min_size
    elif length > max_size:
        return max_size
    return length


def encode_chunk(chunk: bytes, chunk_idx: int) -> dict:
    """
    Encodes a single chunk of data into FEC pieces.

    chunk: The raw bytes of this single chunk.
    chunk_idx: The index of this chunk in the overall file or stream.
    return: A dict containing:
        {
          "blocks": {
            "block_idx": i,
            "block_type": "data" or "parity",
            "block_bytes": block
            },
          "chunk_idx": chunk_idx,
          "k": k,
          "m": m,
          "chunk_size": zfec_chunk_size,
          "padlen": padlen,
          "original_chunk_length": len(chunk)
        }
    """
    piece_size = piece_length(len(chunk))
    bt.logging.debug(
        f"[encode_chunk] chunk {chunk_idx}: {len(chunk)} bytes, piece_size = {piece_size}"
    )

    # Calculate how many data blocks (k) and parity blocks
    expected_data_pieces = math.ceil(len(chunk) / piece_size)
    expected_parity_pieces = math.ceil(expected_data_pieces / 2)

    k = expected_data_pieces
    m = k + expected_parity_pieces

    encoder = Encoder(k, m)
    encoded_blocks = encoder.encode(chunk)

    # Calculate how zfec splits/pads under the hood
    zfec_chunk_size = (len(chunk) + (k - 1)) // k
    padlen = (zfec_chunk_size * k) - len(chunk)

    # block i is data if i < k, parity otherwise
    block_metadata = []
    for i, block in enumerate(encoded_blocks):
        block_type = "data" if i < k else "parity"
        block_metadata.append(
            {"block_idx": i, "block_type": block_type, "block_bytes": block}
        )

    encoded_chunk = {
        "blocks": block_metadata,
        "chunk_idx": chunk_idx,
        "k": k,
        "m": m,
        "chunk_size": zfec_chunk_size,
        "padlen": padlen,
        "original_chunk_length": len(chunk),
    }

    bt.logging.debug(
        f"[encode_chunk] chunk {chunk_idx}: k={k}, m={m}, encoded {len(encoded_blocks)} blocks"
    )
    return encoded_chunk


def decode_chunk(encoded_chunk: dict) -> bytes:
    """
    Decodes a single chunk from the piece dictionary created by encode_chunk.

    encoded_chunk: A dictionary containing:
        {
          "blocks": the list of encoded blocks,
          "k": number of data pieces (threshold),
          "m": total number of pieces,
          "padlen": how many bytes to strip at the end,
          ...
        }
    return: The decoded chunk as bytes.
    """
    k = encoded_chunk["k"]
    m = encoded_chunk["m"]
    padlen = encoded_chunk["padlen"]
    blocks = [b_info["block_bytes"] for b_info in encoded_chunk["blocks"]]

    # zfec decode requires exactly k blocks
    if len(blocks) > k:
        blocks_to_decode = blocks[:k]
        sharenums = list(range(k))
    else:
        blocks_to_decode = blocks
        sharenums = list(range(len(blocks)))

    decoder = Decoder(k, m)
    decoded_chunk = decoder.decode(blocks_to_decode, sharenums, padlen)
    return decoded_chunk


def encode_piece(piece: dict, piece_size: int) -> list[dict]:
    """
    Subdivides an encoded chunk into pieces that are distributed to miners.

    Args:
        piece (dict): A dictionary containing information about the chunk to be subdivided. 
                      It should have the following structure:
                      {
                          "chunk_idx": int,
                          "blocks": [
                              {
                                  "block_bytes": bytes,
                                  "block_type": str,
                                  "block_idx": int
                              },
                              ...
                          ]
        piece_size (int): The size of each piece in bytes.

    Returns:
        list[dict]: A list of dictionaries, each representing a sub-piece of the original chunk.
                    Each dictionary has the following structure:
                    {
                        "chunk_idx": int,
                        "block_idx": int,
                        "block_type": str,
                        "piece_idx": int,
                        "data": bytes
                    }
    """
    pieces = []
    chunk_idx = piece["chunk_idx"]

    for block_info in piece["blocks"]:
        block_bytes = block_info["block_bytes"]
        block_type = block_info["block_type"]
        block_idx = block_info["block_idx"]

        offset = 0
        piece_idx = 0
        while offset < len(block_bytes):
            piece = block_bytes[offset : offset + piece_size]
            offset += piece_size 

            subpiece_info = {
                "chunk_idx": chunk_idx,
                "block_idx": block_idx,
                "block_type": block_type,
                "piece_idx": piece_idx,
                "data": piece,
            }
            pieces.append(subpiece_info)
            piece_idx += 1

    return pieces
