import math
from io import BytesIO
from random import randbytes, sample, shuffle

from fastapi import UploadFile

from storage_subnet.utils.piece import (
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
    real_chunks = 0

    expected_pieces = 0
    pieces = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        print(f"Processing chunk {(chunk_idx)} of size {len(chunk)} bytes")
        real_chunks += 1
        chunk_info = encode_chunk(chunk, chunk_idx)
        print(
            f"Encoded chunk with {chunk_info['k']} data pieces and {chunk_info['m'] - chunk_info['k']} parity pieces "
        )
        piece_size = piece_length(chunk_info["chunk_size"])
        pieces_per_block = math.ceil(chunk_info["chunk_size"] / piece_size)
        num_pieces = chunk_info["m"] * pieces_per_block
        print(f"Expecting number of pieces: {num_pieces}")
        expected_pieces += num_pieces
        piece = encode_pieces(chunk_info, piece_size)
        print(f"Encoded chunk with {len(piece)} pieces of size {piece_size} bytes")
        pieces.extend(piece)

    print(f"Expected number of chunks: {num_chunks}")
    print(f"Generated number of chunks: {real_chunks}")

    print(f"Expected number of pieces: {expected_pieces}")
    print(f"Generated number of pieces: {len(pieces)}")

    assert (
        real_chunks == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {real_chunks}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"


def test_reconstruct_file():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(file=BytesIO(data), filename="test", headers=headers)

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks = []

    expected_pieces = 0
    pieces = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        print(f"Processing chunk {(chunk_idx)} of size {len(chunk)} bytes")
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append({k: v for k, v in chunk_info.items() if k != "blocks"})
        print(
            f"Encoded chunk with {chunk_info['k']} data pieces and {chunk_info['m'] - chunk_info['k']} parity pieces "
        )
        piece_size = piece_length(chunk_info["original_chunk_length"])
        pieces_per_block = math.ceil(chunk_info["chunk_size"] / piece_size)
        num_pieces = chunk_info["m"] * pieces_per_block
        print(f"Expecting number of pieces: {num_pieces}")
        expected_pieces += num_pieces
        piece = encode_pieces(chunk_info, piece_size)
        print(f"Encoded chunk with {len(piece)} pieces of size {piece_size} bytes")
        pieces.extend(piece)

    print(f"Expected number of chunks: {num_chunks}")
    print(f"Generated number of chunks: {len(chunks)}")

    print(f"Expected number of pieces: {expected_pieces}")
    print(f"Generated number of pieces: {len(pieces)}")

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"

    print("Reconstructing chunks...")
    # Jumble the pieces (simulating a distributed scenario)
    shuffle(pieces)

    # Reconstruct the chunks
    reconstructed_chunks = []
    for chunk in chunks:
        chunk_idx = chunk["chunk_idx"]

        # Collect all pieces for this chunk
        relevant_pieces = [piece for piece in pieces if piece["chunk_idx"] == chunk_idx]
        relevant_pieces.sort(key=lambda p: p["block_idx"])
        print(f"Reconstructing chunk {chunk_idx} with {len(relevant_pieces)} pieces")
        for piece in relevant_pieces:
            # Create a new dict omitting the "data" key
            piece_without_data = {k: v for k, v in piece.items() if k != "data"}
            print(piece_without_data)
        # Ensure at least k pieces are available for decoding
        k = chunk["k"]
        if len(relevant_pieces) < k:
            raise ValueError(f"Not enough pieces to reconstruct chunk {chunk_idx}")

        # Decode the chunk
        chunk_blocks = []
        for block_info in relevant_pieces:
            chunk_blocks.append(
                {
                    "block_bytes": block_info["data"],
                    "block_idx": block_info["block_idx"],
                    "block_type": block_info["block_type"],
                }
            )
        chunk["blocks"] = chunk_blocks  # Add blocks back to chunk for decoding
        reconstructed_chunk = decode_chunk(chunk)
        reconstructed_chunks.append(reconstructed_chunk)
        print(f"Reconstructed chunk {chunk_idx} with {len(reconstructed_chunk)} bytes")

    reconstructed_data = b"".join(reconstructed_chunks)
    print("Original length:", len(data))
    print("Reconstructed length:", len(reconstructed_data))

    for i in range(len(data)):
        if data[i] != reconstructed_data[i]:
            print(f"Mismatch at byte offset {i}")
        break

    assert data == reconstructed_data


def test_reconstruct_file_corrupted():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file: UploadFile = UploadFile(file=BytesIO(data), filename="test", headers=headers)

    chunk_size = piece_length(TEST_FILE_SIZE)
    num_chunks = math.ceil(TEST_FILE_SIZE / chunk_size)
    chunks = []

    expected_pieces = 0
    pieces = []
    file.seek(0)

    for chunk_idx, chunk in enumerate(iter(lambda: file.file.read(chunk_size), b"")):
        print(f"Processing chunk {(chunk_idx)} of size {len(chunk)} bytes")
        chunk_info = encode_chunk(chunk, chunk_idx)
        chunks.append({k: v for k, v in chunk_info.items() if k != "blocks"})
        print(
            f"Encoded chunk with {chunk_info['k']} data pieces and {chunk_info['m'] - chunk_info['k']} parity pieces "
        )
        piece_size = piece_length(chunk_info["original_chunk_length"])
        pieces_per_block = math.ceil(chunk_info["chunk_size"] / piece_size)
        num_pieces = chunk_info["m"] * pieces_per_block
        print(f"Expecting number of pieces: {num_pieces}")
        expected_pieces += num_pieces
        piece = encode_pieces(chunk_info, piece_size)
        print(f"Encoded chunk with {len(piece)} pieces of size {piece_size} bytes")
        pieces.extend(piece)

    print(f"Expected number of chunks: {num_chunks}")
    print(f"Generated number of chunks: {len(chunks)}")

    print(f"Expected number of pieces: {expected_pieces}")
    print(f"Generated number of pieces: {len(pieces)}")

    assert (
        len(chunks) == num_chunks
    ), f"Mismatch in chunk counts! Expected {num_chunks}, got {len(chunks)}"
    assert (
        len(pieces) == expected_pieces
    ), f"Mismatch in piece counts! Expected {expected_pieces}, got {len(pieces)}"

    print("Reconstructing chunks...")

    for piece in pieces:
        max_pieces_to_lose = math.ceil(len(pieces) * 0.3)

        num_pieces_to_keep = len(pieces) - max_pieces_to_lose
        keep_pieces = sample(pieces, num_pieces_to_keep)
        keep_blocks = [piece["block_idx"] for piece in keep_pieces]

        pieces = [piece for piece in pieces if piece["block_idx"] in keep_blocks]

    shuffle(pieces)

    # Reconstruct the chunks
    reconstructed_chunks = []
    for chunk in chunks:
        chunk_idx = chunk["chunk_idx"]

        # Collect all pieces for this chunk
        relevant_pieces = [piece for piece in pieces if piece["chunk_idx"] == chunk_idx]
        relevant_pieces.sort(key=lambda p: p["block_idx"])
        print(f"Reconstructing chunk {chunk_idx} with {len(relevant_pieces)} pieces")
        for piece in relevant_pieces:
            # Create a new dict omitting the "data" key
            piece_without_data = {k: v for k, v in piece.items() if k != "data"}
            print(piece_without_data)
        # Ensure at least k pieces are available for decoding
        k = chunk["k"]
        if len(relevant_pieces) < k:
            raise ValueError(f"Not enough pieces to reconstruct chunk {chunk_idx}")

        # Decode the chunk
        chunk_blocks = []
        for block_info in relevant_pieces:
            chunk_blocks.append(
                {
                    "block_bytes": block_info["data"],
                    "block_idx": block_info["block_idx"],
                    "block_type": block_info["block_type"],
                }
            )
        chunk["blocks"] = chunk_blocks  # Add blocks back to chunk for decoding
        reconstructed_chunk = decode_chunk(chunk)
        reconstructed_chunks.append(reconstructed_chunk)
        print(f"Reconstructed chunk {chunk_idx} with {len(reconstructed_chunk)} bytes")

    reconstructed_data = b"".join(reconstructed_chunks)
    print("Original length:", len(data))
    print("Reconstructed length:", len(reconstructed_data))

    for i in range(len(data)):
        if data[i] != reconstructed_data[i]:
            print(f"Mismatch at byte offset {i}")
        break

    assert data == reconstructed_data
