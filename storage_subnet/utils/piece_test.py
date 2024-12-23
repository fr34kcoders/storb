import math
from io import BytesIO
from random import randbytes, sample, shuffle

from fastapi import UploadFile

from storage_subnet.utils.piece import (
    Block,
    EncodedChunk,
    PieceInfo,
    decode_chunk,
    encode_chunk,
    encode_pieces,
    piece_length,
)

TEST_FILE_SIZE = 1024 * 1024


def test_split_file():
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(
        file=BytesIO(randbytes(TEST_FILE_SIZE)), filename="test", headers=headers
    )

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks = []

    expected_pieces = 0
    pieces = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append(chunk_info.model_dump(exclude={"blocks"}))
        piece_size = piece_length(chunk_info.chunk_size)
        pieces_per_block = math.ceil(chunk_info.chunk_size / piece_size)
        num_pieces = chunk_info.m * pieces_per_block
        expected_pieces += num_pieces
        piece = encode_pieces(chunk_info, piece_size)
        pieces.extend(piece)

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"


def test_reconstruct_file():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(file=BytesIO(data), filename="test", headers=headers)

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks: list[EncodedChunk] = []

    expected_pieces = 0
    pieces: list[PieceInfo] = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append(chunk_info.copy(exclude={"blocks"}))
        piece_size = piece_length(chunk_info.original_chunk_length)
        pieces_per_block = math.ceil(chunk_info.chunk_size / piece_size)
        num_pieces = chunk_info.m * pieces_per_block
        expected_pieces += num_pieces
        piece = encode_pieces(chunk_info, piece_size)
        pieces.extend(piece)

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"

    # Jumble the pieces (simulating a distributed scenario)
    shuffle(pieces)

    # Reconstruct the chunks
    reconstructed_chunks = []
    for chunk in chunks:
        chunk_idx = chunk.chunk_idx

        # Collect all pieces for this chunk (TODO: we can optimize this)
        relevant_pieces = [piece for piece in pieces if piece.chunk_idx == chunk_idx]
        relevant_pieces.sort(key=lambda p: p.block_idx)

        # Ensure at least k pieces are available for decoding
        k = chunk.k
        if len(relevant_pieces) < k:
            raise ValueError(f"Not enough pieces to reconstruct chunk {chunk_idx}")

        # Decode the chunk
        chunk_blocks: list[Block] = []
        for block_info in relevant_pieces:
            chunk_blocks.append(
                Block(
                    block_bytes=block_info.data,
                    block_idx=block_info.block_idx,
                    block_type=block_info.block_type,
                )
            )
        chunk.blocks = chunk_blocks  # Add blocks back to chunk for decoding
        reconstructed_chunk = decode_chunk(chunk)
        reconstructed_chunks.append(reconstructed_chunk)

    reconstructed_data = b"".join(reconstructed_chunks)

    assert data == reconstructed_data


def test_reconstruct_file_corrupted():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(file=BytesIO(data), filename="test", headers=headers)

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks: list[EncodedChunk] = []

    expected_pieces = 0
    pieces: list[PieceInfo] = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append(chunk_info.copy(exclude={"blocks"}))
        piece_size = piece_length(chunk_info.original_chunk_length)
        pieces_per_block = math.ceil(chunk_info.chunk_size / piece_size)
        num_pieces = chunk_info.m * pieces_per_block
        expected_pieces += num_pieces
        piece = encode_pieces(chunk_info, piece_size)
        pieces.extend(piece)

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"

    # Randomly drop 30% pieces
    for piece in pieces:
        max_pieces_to_lose = math.ceil(len(pieces) * 0.3)

        num_pieces_to_keep = len(pieces) - max_pieces_to_lose
        keep_pieces = sample(pieces, num_pieces_to_keep)
        keep_blocks = [piece.block_idx for piece in keep_pieces]

        pieces = [piece for piece in pieces if piece.block_idx in keep_blocks]

    shuffle(pieces)

    # Reconstruct the chunks
    reconstructed_chunks: list[EncodedChunk] = []
    for chunk in chunks:
        chunk_idx = chunk.chunk_idx

        # Collect all pieces for this chunk
        relevant_pieces = [piece for piece in pieces if piece.chunk_idx == chunk_idx]
        relevant_pieces.sort(key=lambda p: p.block_idx)
        # Ensure at least k pieces are available for decoding
        k = chunk.k
        if len(relevant_pieces) < k:
            raise ValueError(f"Not enough pieces to reconstruct chunk {chunk_idx}")

        # Decode the chunk
        chunk_blocks: list[Block] = []
        for block_info in relevant_pieces:
            chunk_blocks.append(
                Block(
                    block_bytes=block_info.data,
                    block_idx=block_info.block_idx,
                    block_type=block_info.block_type,
                )
            )
        chunk.blocks = chunk_blocks  # Add blocks back to chunk for decoding
        reconstructed_chunk = decode_chunk(chunk)
        reconstructed_chunks.append(reconstructed_chunk)

    reconstructed_data = b"".join(reconstructed_chunks)

    assert data == reconstructed_data
