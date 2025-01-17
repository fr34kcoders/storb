from typing import Optional

from pydantic import BaseModel, Field

from storb.challenge import Challenge, Proof
from storb.dht.chunk_dht import ChunkDHTValue
from storb.dht.piece_dht import PieceDHTValue
from storb.util.piece import ProcessedPieceInfo


class Store(ProcessedPieceInfo):
    # NOTE: This is currently not used since we are now using raw bytes.
    # This may be removed later
    data: Optional[str] = Field(default=None)  # b64 encoded data

    def __str__(self) -> str:
        return f"Store(piece_type={self.piece_type}, piece_id={self.piece_id})"


class Retrieve(BaseModel):
    piece_id: str  # hash of piece
    piece: Optional[str] = Field(default=None)  # base64 encoded piece


class RetrieveResponse(BaseModel):
    ptype: str
    piece: bytes
    pad_len: int


class StoreResponse(BaseModel):
    infohash: str


class NewChallenge(BaseModel):
    challenge_id: str
    piece_id: str
    validator_id: int
    miner_id: int
    challenge_deadline: str
    public_key: int
    public_exponent: int
    challenge: Challenge
    signature: str


class AckChallenge(BaseModel):
    challenge_id: str
    accept: bool


class ProofResponse(BaseModel):
    challenge_id: str
    piece_id: str
    proof: Proof


class MetadataSynapse(BaseModel):
    infohash: str


class MetadataResponse(BaseModel):
    infohash: str
    filename: str
    timestamp: str
    chunk_size: int
    length: int


class GetMinersBase(BaseModel):
    filename: str
    infohash: str
    chunk_ids: Optional[list[str]] = Field(default=None)
    chunks_metadata: Optional[list[ChunkDHTValue]] = Field(
        default=None
    )  # list of chunk metadata
    pieces_metadata: Optional[list[list[PieceDHTValue]]] = Field(
        default=None
    )  # multi dimensional array of piece metadata. each row corresponds to a chunk
