from typing import Union

from kademlia.storage import IStorage

from storage_subnet.dht.chunk_dht import ChunkDHTValue
from storage_subnet.dht.piece_dht import PieceDHTValue
from storage_subnet.dht.tracker_dht import TrackerDHTValue
from storage_subnet.validator import db
from storage_subnet.validator.db import ChunkEntry, PieceEntry, TrackerEntry

DHTValue = Union[ChunkDHTValue, PieceDHTValue, TrackerDHTValue]


class SqliteStorageDHT(IStorage):
    async def __init__(self):
        self.db_dir = db.DB_DIR
        self.db_conn = await db.get_db_connection(self.db_dir)

    async def __setitem__(self, key: str, value: str):
        """
        Arguments:
            key (str): The namespaced key in the form `<namespace>:<id>`
            value (str): JSON string of the values which should match one of the schemas:
                - ChunkDHTValue
                - PieceDHTValue
                - TrackerDHTValue
        """
        key_namespace, key_id = key.split(":")
        match key_namespace:
            case "chunk":
                val = ChunkDHTValue.model_validate_json(value)
                entry = ChunkEntry(
                    chunk_hash=key_id,
                    infohash=val.infohash,
                    validator_id=val.validator_id,
                    piece_hashes=val.piece_hashes,
                    chunk_idx=val.chunk_idx,
                    k=val.k,
                    m=val.m,
                    chunk_size=val.chunk_size,
                    padlen=val.padlen,
                    original_chunk_size=val.original_chunk_size,
                    signature=val.signature,
                )
                await db.set_chunk_entry(self.db_conn, entry)
            case "piece":
                val = PieceDHTValue.model_validate_json(value)
                entry = PieceEntry(
                    piece_hash=key_id,
                    chunk_hash=val.chunk_hash,
                    miner_id=val.miner_id,
                    chunk_idx=val.chunk_idx,
                    piece_idx=val.piece_idx,
                    piece_type=val.piece_type,
                    signature=val.signature,
                )
                await db.set_piece_entry(self.db_conn, entry)
            case "tracker":
                val = TrackerDHTValue.model_validate_json(value)
                entry = TrackerEntry(
                    infohash=key_id,
                    validator_id=val.validator_id,
                    filename=val.filename,
                    length=val.length,
                    chunk_length=val.chunk_length,
                    chunk_count=val.chunk_count,
                    chunk_hashes=val.chunk_hashes,
                    creation_timestamp=val.creation_timestamp,
                    signature=val.signature,
                )
                await db.set_tracker_entry(self.db_conn, entry)
            case _:
                raise ValueError(f"Invalid key namespace: {key_namespace}")

    async def __getitem__(self, key: str) -> DHTValue:
        """
        Arguments:
            key (str): The namespaced key in the form `<namespace>:<id>`

        Returns:
            DHTValue
        """
        key_namespace, key_id = key.split(":")
        match key_namespace:
            case "chunk":
                entry = await db.get_chunk_entry(self.db_conn, key_id)
                value = ChunkDHTValue(
                    infohash=entry.infohash,
                    validator_id=entry.validator_id,
                    piece_hashes=entry.piece_hashes,
                    chunk_idx=entry.chunk_idx,
                    k=entry.k,
                    m=entry.m,
                    chunk_size=entry.chunk_size,
                    padlen=entry.padlen,
                    original_chunk_size=entry.original_chunk_size,
                    signature=entry.signature,
                )
                return value
            case "piece":
                entry = await db.get_piece_entry(self.db_conn, key_id)
                value = PieceDHTValue(
                    chunk_hash=entry.chunk_hash,
                    miner_id=entry.miner_id,
                    chunk_idx=entry.chunk_idx,
                    piece_idx=entry.piece_idx,
                    piece_type=entry.piece_type,
                    signature=entry.signature,
                )
                return value
            case "tracker":
                entry = await db.get_tracker_entry(self.db_conn, key_id)
                value = TrackerDHTValue(
                    validator_id=entry.validator_id,
                    filename=entry.filename,
                    length=entry.length,
                    chunk_length=entry.chunk_length,
                    chunk_count=entry.chunk_count,
                    chunk_hashes=entry.chunk_hashes,
                    creation_timestamp=entry.creation_timestamp,
                    signature=entry.signature,
                )
                return value
            case _:
                raise ValueError(f"Invalid key namespace {key_namespace}")

    async def delete(self, key: str):
        key_namespace, key_id = key.split(":")
        match key_namespace:
            case "chunk":
                await db.delete_chunk_entry(self.db_conn, key_id)
            case "piece":
                await db.delete_piece_entry(self.db_conn, key_id)
            case "tracker":
                await db.delete_tracker_entry(self.db_conn, key_id)
            case _:
                raise ValueError(f"Invalid key namespace {key_namespace}")
