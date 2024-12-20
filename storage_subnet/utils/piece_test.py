from io import BytesIO
from random import randbytes

from fastapi import UploadFile

from storage_subnet.constants import EC_DATA_SIZE, EC_PARITY_SIZE
from storage_subnet.utils.piece import piece_length, reconstruct_file, split_file

TEST_FILE_SIZE = 1024 * 1024


def test_split_file():
    headers = {"content-type": "application/octet-stream"}
    file = UploadFile(
        file=BytesIO(randbytes(TEST_FILE_SIZE)), filename="test", headers=headers
    )
    plen = piece_length(TEST_FILE_SIZE)
    gen = split_file(file, plen)

    assert len(list(gen)) == EC_DATA_SIZE + EC_PARITY_SIZE


def test_reconstruct_file():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file = UploadFile(file=BytesIO(data), filename="test", headers=headers)
    plen = piece_length(TEST_FILE_SIZE)
    gen = split_file(file, plen)

    file_pieces = list(gen)

    assert len(file_pieces) == EC_DATA_SIZE + EC_PARITY_SIZE

    reconstructed_data = reconstruct_file(file_pieces, EC_DATA_SIZE, EC_PARITY_SIZE)

    assert data == reconstructed_data


def test_reconstruct_file_corrupted():
    data = randbytes(TEST_FILE_SIZE)
    headers = {"content-type": "application/octet-stream"}
    file = UploadFile(file=BytesIO(data), filename="test", headers=headers)
    plen = piece_length(TEST_FILE_SIZE)
    gen = split_file(file, plen)

    file_pieces = list(gen)
    assert len(file_pieces) == EC_DATA_SIZE + EC_PARITY_SIZE

    # Delete the first `pieces_to_lose` pieces. They will be removed from the start of
    # the list, so they will be data pieces.
    pieces_to_lose = int(len(file_pieces) * 0.3)
    file_pieces = file_pieces[pieces_to_lose:]

    reconstructed_data = reconstruct_file(file_pieces, EC_DATA_SIZE, EC_PARITY_SIZE)

    assert data == reconstructed_data
