from pydantic import BaseModel

from storb.util.piece import PieceType


class PieceDHTValue(BaseModel):
    piece_hash: str
    validator_id: int
    miner_id: set[int] | str
    chunk_idx: int
    piece_idx: int
    piece_type: PieceType
    tag: str
    signature: str

    def to_dict(self) -> dict:
        return self.model_dump()
