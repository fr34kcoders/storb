import asyncio
import json
from datetime import datetime

import bittensor as bt

from storage_subnet.dht.base_dht import BaseDHT


class TrackerDHTValue:
    def __init__(
        self,
        validator_id: int,
        filename: str,
        length: int,
        piece_length: int,
        piece_count: int,
        parity_count: int,
    ):
        if length <= 0:
            raise ValueError("File length must be greater than 0.")
        if piece_length <= 0:
            raise ValueError("Piece length must be greater than 0.")
        if piece_count <= 0:
            raise ValueError("Piece count must be greater than 0.")
        if parity_count < 0:
            raise ValueError("Parity count cannot be negative.")

        self.validator_id = validator_id
        self.filename = filename
        self.creation_timestamp = datetime.now().isoformat()
        self.updated_timestamp = self.creation_timestamp
        self.piece_length = piece_length
        self.piece_count = piece_count
        self.parity_count = parity_count
        self.length = length

    def to_dict(self) -> dict[str, str | int | list[str]]:
        return {
            "validator_id": self.validator_id,
            "filename": self.filename,
            "creation_timestamp": self.creation_timestamp,
            "updated_timestamp": self.updated_timestamp,
            "piece_length": self.piece_length,
            "piece_count": self.piece_count,
            "parity_count": self.parity_count,
            "length": self.length,
        }

    def update_timestamp(self):
        self.updated_timestamp = datetime.now().isoformat()


class TrackerDHT(BaseDHT):
    async def store_tracker_entry(self, infohash: str, value: TrackerDHTValue) -> None:
        """
        Store a tracker entry in the DHT.

        Args:
            infohash (str): The unique identifier for the file.
            value (TrackerDHTValue): The tracker value to store.
        """
        bt.logging.info(f"Storing tracker entry for infohash: {infohash}")
        ser_value = json.dumps(value.to_dict())
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(f"tracker:{infohash}", ser_value),
                self.loop,
            )
            res = future.result(timeout=5)  # Optional timeout
            if not res:
                raise RuntimeError(f"Failed to store tracker entry for {infohash}")
            bt.logging.info(f"Successfully stored tracker entry for {infohash}")
        except Exception as e:
            raise RuntimeError(f"Failed to store tracker entry for {infohash}: {e}")

    async def get_tracker_entry(
        self, infohash: str
    ) -> dict[str, str | int | list[str]]:
        """
        Retrieve a tracker entry from the DHT.

        Args:
            infohash (str): The unique identifier for the file.

        Returns:
            Dict: The tracker value associated with the infohash.
        """
        bt.logging.info(f"Retrieving tracker entry for infohash: {infohash}")
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.get(f"tracker:{infohash}"), self.loop
            )
            ser_value = future.result(timeout=5)  # Optional timeout
            if ser_value is None:
                raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}")
            value = json.loads(ser_value)
            bt.logging.info(f"Successfully retrieved tracker entry for {infohash}")
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}: {e}")
