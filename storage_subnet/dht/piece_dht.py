import asyncio

import bittensor as bt
from pydantic import BaseModel

from storage_subnet.dht.base_dht import BaseDHT


class PieceDHTValue(BaseModel):
    miner_id: int
    piece_type: str

    def to_dict(self) -> dict:
        return self.model_dump()


class PieceDHT(BaseDHT):
    def __init__(self, port: int = 6942, bootstrap_node: tuple[str, int] = None):
        super().__init__(port=port, startup_timeout=10)
        self.bootstrap_node = bootstrap_node

    async def store_piece_entry(self, piece_id: str, value: PieceDHTValue) -> None:
        """
        Store a piece entry in the DHT.
        """
        bt.logging.info(f"Storing piece entry for piece_id: {piece_id}")
        ser_value = value.model_dump_json()
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(f"piece:{piece_id}", ser_value),
                self.loop,
            )
            res = future.result(timeout=5)
            if not res:
                raise RuntimeError(f"Failed to store piece entry for {piece_id}")
            bt.logging.info(f"Successfully stored piece entry for {piece_id}")
        except Exception as e:
            raise RuntimeError(f"Failed to store piece entry for {piece_id}: {e}")

    async def get_piece_entry(self, piece_id: str) -> PieceDHTValue:
        """
        Retrieve a piece entry from the DHT.
        """
        bt.logging.info(f"Retrieving piece entry for piece_id: {piece_id}")
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.get(f"piece:{piece_id}"), self.loop
            )
            ser_value = future.result(timeout=5)
            if ser_value is None:
                raise RuntimeError(f"Failed to retrieve piece entry for {piece_id}")
            value = PieceDHTValue.model_validate_json(ser_value)
            bt.logging.info(f"Successfully retrieved piece entry for {piece_id}")
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve piece entry for {piece_id}: {e}")
