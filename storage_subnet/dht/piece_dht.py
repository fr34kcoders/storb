from pydantic import BaseModel


class PieceDHTValue(BaseModel):
    miner_id: int
    piece_type: str
    pad_len: int

    def to_dict(self) -> dict:
        return self.model_dump()
