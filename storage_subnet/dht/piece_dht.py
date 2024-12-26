from pydantic import BaseModel

from storage_subnet.utils.piece import PieceType

# infohash -> ChunkDHTValue -> PieceDHTValue


class PieceDHTValue(BaseModel):
    chunk_hash: str
    miner_id: int
    chunk_idx: int
    piece_idx: int
    piece_type: PieceType
    signature: str

    def to_dict(self) -> dict:
        return self.model_dump()
