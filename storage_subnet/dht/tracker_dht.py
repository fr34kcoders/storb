from datetime import datetime
from typing import Union

from pydantic import BaseModel, Field, field_validator


class TrackerDHTValue(BaseModel):
    validator_id: int
    filename: str
    length: int = Field(..., gt=0, description="File length must be greater than 0.")
    piece_length: int = Field(
        ..., gt=0, description="Piece length must be greater than 0."
    )
    piece_count: int = Field(
        ..., gt=0, description="Piece count must be greater than 0."
    )
    parity_count: int = Field(..., ge=0, description="Parity count cannot be negative.")
    creation_timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_timestamp: Union[str, None] = None

    @field_validator("filename")
    @classmethod
    def validate_filename(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Filename cannot be empty.")
        return value

    def to_dict(self) -> dict:
        return self.model_dump()

    def update_timestamp(self):
        self.updated_timestamp = datetime.now().isoformat()
