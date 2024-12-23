from pydantic import BaseModel

from storage_subnet.utils.piece import PieceType

# infohash -> ChunkDHTValue -> PieceDHTValue

class PieceDHTValue(BaseModel):
    miner_id: int
    chunk_idx: int
    piece_idx: int
    piece_type: PieceType

    def to_dict(self) -> dict:
        return self.model_dump()

class ChunkDHTValue(BaseModel):
    piece_hashes: list[str]
    chunk_idx: int
    k: int
    m: int
    chunk_size: int
    padlen: int
    original_chunk_length: int

    def to_dict(self) -> dict:
        return self.model_dump()
