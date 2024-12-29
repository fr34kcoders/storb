from pydantic import BaseModel


class ChunkDHTValue(BaseModel):
    chunk_hash: str
    validator_id: int
    piece_hashes: list[str]
    chunk_idx: int
    k: int
    m: int
    chunk_size: int
    padlen: int
    original_chunk_size: int
    signature: str

    def to_dict(self) -> dict:
        return self.model_dump()
