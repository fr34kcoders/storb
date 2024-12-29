from typing import Optional

from pydantic import BaseModel, Field

from storb.dht.piece_dht import ChunkDHTValue, PieceDHTValue
from storb.util.piece import ProcessedPieceInfo


class Store(ProcessedPieceInfo):
    # NOTE: This is currently not used since we are now using raw bytes.
    # This may be removed later
    data: Optional[str] = Field(default=None)  # b64 encoded data

    def __str__(self) -> str:
        return f"Store(piece_type={self.piece_type}, pad_len={self.pad_len}, piece_id={self.piece_id})"

    def preview_no_piece(self) -> str:
        return f"Store(piece_type={self.piece_type},  piece_id={self.piece_id})"


class Retrieve(BaseModel):
    piece_id: str  # hash of piece
    piece: Optional[str] = Field(default=None)  # base64 encoded piece


class RetrieveResponse(BaseModel):
    ptype: str
    piece: bytes
    pad_len: int


class StoreResponse(BaseModel):
    infohash: str


class MetadataSynapse(BaseModel):
    infohash: str


class MetadataResponse(BaseModel):
    infohash: str
    filename: str
    timestamp: str
    piece_length: int
    length: int


class GetMinersBase(BaseModel):
    infohash: str
    chunk_ids: Optional[list[str]] = Field(default=None)
    chunks_metadata: Optional[list[ChunkDHTValue]] = Field(
        default=None
    )  # list of chunk metadata
    pieces_metadata: Optional[list[list[PieceDHTValue]]] = Field(
        default=None
    )  # multi dimensional array of piece metadata. each row corresponds to a chunk


class GetMiners(GetMinersBase):
    def __str__(self) -> str:
        return f"GetMiners(infohash={self.infohash}, \
            chunk_ids={self.chunk_ids}, \
            chunks_metadata={self.chunks_metadata}, \
            pieces_metadata={self.pieces_metadata})"
