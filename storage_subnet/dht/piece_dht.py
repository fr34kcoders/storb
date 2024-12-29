from pydantic import BaseModel

from storage_subnet.utils.piece import PieceType


class PieceDHTValue(BaseModel):
    piece_hash: str
    miner_id: int
    chunk_idx: int
    piece_idx: int
    piece_type: PieceType
    signature: str

    def to_dict(self) -> dict:
        return self.model_dump()
