from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class TrackerDHTValue(BaseModel):
    validator_id: int
    filename: str
    length: int = Field(..., gt=0, description="File length must be greater than 0.")
    chunk_length: int = Field(
        ..., gt=0, description="Chunk length must be greater than 0."
    )
    chunk_count: int = Field(
        ..., gt=0, description="Chunk count must be greater than 0."
    )
    chunk_hashes: list[str] = Field(
        ..., description="List of chunk hashes.", min_items=1
    )
    creation_timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    @field_validator("filename")
    @classmethod
    def validate_filename(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Filename cannot be empty.")
        return value

    def to_dict(self) -> dict:
        return self.model_dump()
