# The MIT License (MIT)
# Copyright (c) 2023 Yuma Rao
# Copyright (c) 2024 𝓯𝓻𝓮𝓪𝓴𝓬𝓸𝓭𝓮𝓻𝓼

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import typing

import bittensor as bt
from pydantic import BaseModel, Field

from storage_subnet.dht.chunk_dht import ChunkDHTValue
from storage_subnet.dht.piece_dht import PieceDHTValue
from storage_subnet.utils.piece import ProcessedPieceInfo


class Store(bt.Synapse, ProcessedPieceInfo):
    data: typing.Optional[str] = Field(default=None)  # b64 encoded data

    def __str__(self) -> str:
        return f"Store(piece_type={self.piece_type}, pad_len={self.pad_len}, piece_id={self.piece_id})"

    def preview_no_piece(self) -> str:
        return f"Store(piece_type={self.piece_type},  piece_id={self.piece_id})"


class Retrieve(bt.Synapse):
    piece_id: str  # hash of piece
    piece: typing.Optional[str] = Field(default=None)  # base64 encoded piece


class RetrieveResponse(bt.Synapse):
    ptype: str
    piece: bytes
    pad_len: int


class StoreResponse(BaseModel):
    infohash: str


class MetadataSynapse(bt.Synapse):
    infohash: str


class MetadataResponse(bt.Synapse):
    infohash: str
    filename: str
    timestamp: str
    piece_length: int
    length: int


class GetMinersBase(BaseModel):
    infohash: str
    chunk_ids: typing.Optional[list[str]] = Field(default=None)
    chunks_metadata: typing.Optional[list[ChunkDHTValue]] = Field(
        default=None
    )  # list of chunk metadata
    pieces_metadata: typing.Optional[list[list[PieceDHTValue]]] = Field(
        default=None
    )  # multi dimensional array of piece metadata. each row corresponds to a chunk


class GetMiners(bt.Synapse, GetMinersBase):
    def __str__(self) -> str:
        return f"GetMiners(infohash={self.infohash}, \
            chunk_ids={self.chunk_ids}, \
            chunks_metadata={self.chunks_metadata}, \
            pieces_metadata={self.pieces_metadata})"


class Dummy(bt.Synapse):
    """
    A simple dummy protocol representation which uses bt.Synapse as its base.
    This protocol helps in handling dummy request and response communication between
    the miner and the validator.

    Attributes:
    - dummy_input: An integer value representing the input request sent by the validator.
    - dummy_output: An optional integer value which, when filled, represents the response from the miner.
    """

    # Required request input, filled by sending dendrite caller.
    dummy_input: int

    # Optional request output, filled by receiving axon.
    dummy_output: typing.Optional[int] = None

    def deserialize(self) -> int:
        """
        Deserialize the dummy output. This method retrieves the response from
        the miner in the form of dummy_output, deserializes it and returns it
        as the output of the dendrite.query() call.

        Returns:
        - int: The deserialized response, which in this case is the value of dummy_output.

        Example:
        Assuming a Dummy instance has a dummy_output value of 5:
        >>> dummy_instance = Dummy(dummy_input=4)
        >>> dummy_instance.dummy_output = 5
        >>> dummy_instance.deserialize()
        5
        """
        return self.dummy_output
