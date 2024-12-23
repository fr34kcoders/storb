import math
from io import BytesIO
from random import randbytes, sample, shuffle

from fastapi import UploadFile

from storage_subnet.utils.piece import (
    EncodedChunk,
    Piece,
    encode_chunk,
    piece_length,
    reconstruct_data,
)

TEST_FILE_SIZE = 1024 * 1024


def test_split_file():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(file=BytesIO(data), filename="test", headers=headers)

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks: list[EncodedChunk] = []

    expected_pieces = 0
    pieces: list[Piece] = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append(chunk_info.copy(exclude={"blocks"}))
        piece_size = piece_length(chunk_info.original_chunk_length)
        pieces_per_block = math.ceil(chunk_info.chunk_size / piece_size)
        num_pieces = chunk_info.m * pieces_per_block
        expected_pieces += num_pieces
        pieces.extend(chunk_info.pieces)

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
    pieces: list[Piece] = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append(chunk_info.copy(exclude={"blocks"}))
        piece_size = piece_length(chunk_info.original_chunk_length)
        pieces_per_block = math.ceil(chunk_info.chunk_size / piece_size)
        num_pieces = chunk_info.m * pieces_per_block
        expected_pieces += num_pieces
        pieces.extend(chunk_info.pieces)

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"

    shuffle(pieces)


    reconstructed_data = reconstruct_data(pieces, chunks)
    assert data == reconstructed_data, "Data mismatch!"


def test_reconstruct_file_corrupted():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(file=BytesIO(data), filename="test", headers=headers)

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks: list[EncodedChunk] = []

    expected_pieces = 0
    pieces: list[Piece] = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append(chunk_info.copy(exclude={"blocks"}))
        piece_size = piece_length(chunk_info.original_chunk_length)
        pieces_per_block = math.ceil(chunk_info.chunk_size / piece_size)
        num_pieces = chunk_info.m * pieces_per_block
        expected_pieces += num_pieces
        pieces.extend(chunk_info.pieces)

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"


    # Randomly drop 30% pieces
    for _ in pieces:
        max_pieces_to_lose = math.ceil(len(pieces) * 0.3)

        num_pieces_to_keep = len(pieces) - max_pieces_to_lose
        keep_pieces = sample(pieces, num_pieces_to_keep)
        keep_blocks = [piece.piece_idx for piece in keep_pieces]

        pieces = [piece for piece in pieces if piece.piece_idx in keep_blocks]

    shuffle(pieces)


    reconstructed_data = reconstruct_data(pieces, chunks)
    assert data == reconstructed_data, "Data mismatch!"
