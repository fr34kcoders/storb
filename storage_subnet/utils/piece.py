import hashlib
import math
import typing
from collections.abc import Iterator
from enum import IntEnum

import bittensor as bt
from pydantic import BaseModel, Field
from zfec.easyfec import Decoder, Encoder

from storage_subnet.constants import (
    MIN_PIECE_SIZE,
    PIECE_LENGTH_OFFSET,
    PIECE_LENGTH_SCALING,
)


class PieceType(IntEnum):
    Data = 0
    Parity = 1


class Piece(BaseModel):
    class Config:
        use_enum_values = True

    chunk_idx: int
    piece_idx: int
    piece_type: PieceType
    data: bytes


class EncodedChunk(BaseModel):
    pieces: list[Piece] = Field(default=None, required=False)
    chunk_idx: int
    k: int  # Number of data blocks
    m: int  # Total blocks (data + parity)
    chunk_size: int
    padlen: int
    original_chunk_size: int


class ProcessedPieceInfo(Piece):
    piece_id: typing.Optional[str] = Field(default=None)


class EncodedPieces(BaseModel):
    pieces: list[Piece]


def piece_hash(data: bytes) -> str:
    """Calculate the SHA-1 hash of a piece of data.

    Args:
        data (bytes): The data to hash.

    Returns:
        str: The SHA-1 hash of the data.
    """
    return hashlib.sha1(data).hexdigest()


def piece_length(content_length: int, min_size: int = MIN_PIECE_SIZE) -> int:
    """Calculate an approximate piece size based on content length,
    clamped to a [min_size..max_size] range."""
    exponent = int(
        (math.log2(content_length) * PIECE_LENGTH_SCALING) + PIECE_LENGTH_OFFSET
    )
    length = 1 << exponent
    if length < min_size:
        return min_size
    return length


def encode_chunk(chunk: bytes, chunk_idx: int) -> EncodedChunk:
    """
    Encodes a single chunk of data into FEC pieces.

    Arguments
        chunk (bytes): The raw bytes of this single chunk.
        chunk_idx (int): The index of this chunk in the overall file or stream.

    Returns:
        EncodedChunk: An EncodedChunk object.
    """
    chunk_size = len(chunk)
    piece_size = piece_length(chunk_size)
    bt.logging.debug(
        f"[encode_chunk] chunk {chunk_idx}: {chunk_size} bytes, piece_size = {piece_size}"
    )

    # Calculate how many data blocks (k) and parity blocks
    expected_data_pieces = math.ceil(chunk_size / piece_size)
    expected_parity_pieces = math.ceil(expected_data_pieces / 2)

    k = expected_data_pieces
    m = k + expected_parity_pieces

    encoder = Encoder(k, m)
    encoded_pieces = encoder.encode(chunk)

    # Calculate how zfec splits/pads under the hood
    zfec_chunk_size = (chunk_size + (k - 1)) // k
    padlen = (zfec_chunk_size * k) - chunk_size

    # block i is data if i < k, parity otherwise
    pieces = []
    for i, piece in enumerate(encoded_pieces):
        piece_type = PieceType.Data if i < k else PieceType.Parity
        print(f"Encoding piece {i} with length {len(piece)}")
        pieces.append(
            Piece(
                piece_type=piece_type,
                data=piece,
                chunk_idx=chunk_idx,
                piece_idx=i,
            )
        )

    for piece in pieces:
        print(f"[encode] Piece length: {len(piece.data)}")

    encoded_chunk = EncodedChunk(
        pieces=pieces,
        chunk_idx=chunk_idx,
        k=k,
        m=m,
        chunk_size=zfec_chunk_size,
        padlen=padlen,
        original_chunk_size=chunk_size,
    )

    bt.logging.debug(
        f"[encode_chunk] chunk {chunk_idx}: k={k}, m={m}, encoded {len(encoded_chunk.pieces)} blocks"
    )
    return encoded_chunk


def decode_chunk(encoded_chunk: EncodedChunk) -> bytes:
    """
    Decodes a single chunk from the piece dictionary created by encode_chunk.

    Arguments:
        encoded_chunk (EncodedChunk): An EncodedChunk object.

    Returns:
        bytes: The decoded chunk as bytes.
    """
    k = encoded_chunk.k
    m = encoded_chunk.m
    padlen = encoded_chunk.padlen
    pieces = [p.data for p in encoded_chunk.pieces]

    # zfec decode requires exactly k blocks
    if len(pieces) > k:
        pieces_to_decode = pieces[:k]
        sharenums = list(range(k))
    else:
        pieces_to_decode = pieces
        sharenums = list(range(len(pieces)))

    decoder = Decoder(k, m)
    decoded_chunk = decoder.decode(pieces_to_decode, sharenums, padlen)
    return decoded_chunk


def reconstruct_data(pieces: list[Piece], chunks: list[EncodedChunk]) -> bytes:
    """
    Reconstructs the original data from chunks

    Arguments:
        pieces (list[Piece]): List of pieces from the miners
        chunks (list[EncodedChunk]): List of encoded chunks from the validators

    Returns:
        bytes: The original data in bytes
    """
    reconstructed_chunks = []

    for chunk in chunks:
        chunk_idx = chunk.chunk_idx

        # Collect all pieces for this chunk (TODO: we can optimize this)
        relevant_pieces = [piece for piece in pieces if piece.chunk_idx == chunk_idx]
        relevant_pieces.sort(key=lambda p: p.piece_idx)

        # Ensure at least k pieces are available for decoding
        k = chunk.k
        if len(relevant_pieces) < k:
            raise ValueError(f"Not enough pieces to reconstruct chunk {chunk_idx}")

        chunk.pieces = relevant_pieces
        reconstructed_chunk = decode_chunk(chunk)
        reconstructed_chunks.append(reconstructed_chunk)

    reconstructed_data = b"".join(reconstructed_chunks)
    return reconstructed_data


def reconstruct_data_stream(
    pieces: list[Piece], chunks: list[EncodedChunk]
) -> Iterator[bytes]:
    """
    Generator that yields the reconstructed data chunk by chunk.
    Each yield returns the decoded bytes for a single chunk.
    """

    for chunk in chunks:
        chunk_idx = chunk.chunk_idx

        # Collect all pieces for this chunk (TODO: we can optimize this)
        relevant_pieces = [piece for piece in pieces if piece.chunk_idx == chunk_idx]
        relevant_pieces.sort(key=lambda p: p.piece_idx)

        # Ensure at least k pieces are available for decoding
        k = chunk.k
        if len(relevant_pieces) < k:
            raise ValueError(f"Not enough pieces to reconstruct chunk {chunk_idx}")

        # Attach the relevant pieces to the chunk object for decoding
        chunk.pieces = relevant_pieces

        # Decode the chunk
        reconstructed_chunk = decode_chunk(chunk)
        yield reconstructed_chunk
