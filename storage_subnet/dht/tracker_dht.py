import asyncio
from datetime import datetime
from typing import Union

import bittensor as bt
from pydantic import BaseModel, Field, field_validator

from storage_subnet.dht.base_dht import BaseDHT


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


class TrackerDHT(BaseDHT):
    def __init__(self, port: int = 6942, bootstrap_node: tuple[str, int] = None):
        super().__init__(port=port, startup_timeout=10)
        self.bootstrap_node = bootstrap_node

    async def store_tracker_entry(self, infohash: str, value: TrackerDHTValue) -> None:
        """
        Store a tracker entry in the DHT.
        """
        bt.logging.info(f"Storing tracker entry for infohash: {infohash}")
        ser_value = value.model_dump_json()
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(f"tracker:{infohash}", ser_value),
                self.loop,
            )
            res = future.result(timeout=5)
            if not res:
                raise RuntimeError(f"Failed to store tracker entry for {infohash}")
            bt.logging.info(f"Successfully stored tracker entry for {infohash}")
        except Exception as e:
            raise RuntimeError(f"Failed to store tracker entry for {infohash}: {e}")

    async def get_tracker_entry(self, infohash: str) -> TrackerDHTValue:
        """
        Retrieve a tracker entry from the DHT.
        """
        bt.logging.info(f"Retrieving tracker entry for infohash: {infohash}")
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.get(f"tracker:{infohash}"), self.loop
            )
            ser_value = future.result(timeout=5)
            bt.logging.info(f"Retrieved tracker entry for infohash: {ser_value}")
            if not ser_value:
                raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}")
            value = TrackerDHTValue.model_validate_json(ser_value)
            bt.logging.info(f"Successfully retrieved tracker entry for {infohash}")
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}: {e}")
