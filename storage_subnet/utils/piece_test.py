from io import BytesIO
from random import randbytes

from fastapi import UploadFile

from ..constants import EC_DATA_SIZE, EC_PARITY_SIZE
from .piece import piece_length, reconstruct_file, split_file

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
