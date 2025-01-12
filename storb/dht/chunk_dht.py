from typing import Optional

from pydantic import BaseModel


class ChunkDHTValue(BaseModel):
    chunk_hash: Optional[str] = None  # Chunk hash is calculated from the chunk data
    validator_id: int
    piece_hashes: list[str]
    chunk_idx: int
    k: int
    m: int
    chunk_size: int
    padlen: int
    original_chunk_size: int
    signature: Optional[str] = None  # signature is calculated from the chunk data

    def to_dict(self) -> dict:
        return self.model_dump()
