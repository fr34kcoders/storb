"""
Provides functions for interacting with validator's database
"""

import json
from contextlib import asynccontextmanager

import aiosqlite
from pydantic import BaseModel

from storage_subnet.utils.piece import PieceType

DB_DIR = "validator_database.db"
PIECE_ID_MINER_UIDS = "piece_id_miner_uids"
METADATA = "metadata"
INFOHASH_CHUNK_IDS = "infohash_chunk_ids"


class ChunkEntry(BaseModel):
    chunk_hash: str
    infohash: str
    validator_id: int
    piece_hashes: list[str]
    chunk_idx: int
    k: int
    m: int
    chunk_size: int
    padlen: int
    original_chunk_size: int
    signature: str


class PieceEntry(BaseModel):
    piece_hash: str
    chunk_hash: str
    miner_id: int
    chunk_idx: int
    piece_idx: int
    piece_type: PieceType
    signature: str


class TrackerEntry(BaseModel):
    infohash: str
    validator_id: int
    filename: str
    length: int
    chunk_length: int
    chunk_count: int
    chunk_hashes: list[str]
    creation_timestamp: str
    signature: str


@asynccontextmanager
async def get_db_connection(db_dir: str, uri: bool = False):  # noqa: ANN201
    conn = await aiosqlite.connect(db_dir, uri=uri)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
    try:
        yield conn  # Provide the connection to the calling code
    finally:
        await conn.close()  # Ensure the connection is properly closed


async def get_miner_stats(conn: aiosqlite.Connection, miner_uid: int):
    query = """
    SELECT * FROM miner_stats
    WHERE miner_uid = ?
    """
    async with conn.execute(query, (miner_uid,)) as cursor:
        row = await cursor.fetchone()
        if row:
            return {
                "miner_uid": row[0],
                "challenge_successes": row[1] or 0,
                "challenge_attempts": row[2] or 0,
                "retrieval_successes": row[3] or 0,
                "retrieval_attempts": row[4] or 0,
                "store_successes": row[5] or 0,
                "store_attempts": row[6] or 0,
                "total_successes": row[7] or 0,
            }
        return None  # Miner not found


async def get_multiple_miner_stats(conn: aiosqlite.Connection, miner_uids: list[int]):
    placeholders = ",".join("?" for _ in miner_uids)
    query = f"""
    SELECT * FROM miner_stats
    WHERE miner_uid IN ({placeholders})
    """
    async with conn.execute(query, miner_uids) as cursor:
        rows = await cursor.fetchall()
        return {
            row[0]: {
                "miner_uid": row[0],
                "challenge_successes": row[1] or 0,
                "challenge_attempts": row[2] or 0,
                "retrieval_successes": row[3] or 0,
                "retrieval_attempts": row[4] or 0,
                "store_successes": row[5] or 0,
                "store_attempts": row[6] or 0,
                "total_successes": row[7] or 0,
            }
            for row in rows
        }


async def get_all_miner_stats(conn: aiosqlite.Connection):
    query = "SELECT * FROM miner_stats"
    async with conn.execute(query) as cursor:
        rows = await cursor.fetchall()
        return {
            row[0]: {
                "miner_uid": row[0],
                "challenge_successes": row[1] or 0,
                "challenge_attempts": row[2] or 0,
                "retrieval_successes": row[3] or 0,
                "retrieval_attempts": row[4] or 0,
                "store_successes": row[5] or 0,
                "store_attempts": row[6] or 0,
                "total_successes": row[7] or 0,
            }
            for row in rows
        }


async def update_stats(conn: aiosqlite.Connection, miner_uid: int, stats: dict):
    updates = ", ".join([f"{key} = ?" for key in stats.keys()])
    values = list(stats.values()) + [miner_uid]
    query = f"UPDATE miner_stats SET {updates} WHERE miner_uid = ?"
    await conn.execute(query, values)
    await conn.commit()


async def set_tracker_entry(conn: aiosqlite.Connection, entry: TrackerEntry):
    query = """
    INSERT INTO tracker (infohash, validator_id, filename, length, chunk_length, chunk_count, chunk_hashes, creation_timestamp, signature)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    await conn.execute(
        query,
        (
            entry.infohash,
            entry.validator_id,
            entry.filename,
            entry.length,
            entry.chunk_length,
            entry.chunk_count,
            json.dumps(entry.chunk_hashes),
            entry.creation_timestamp,
            entry.signature,
        ),
    )
    await conn.commit()


async def get_tracker_entry(conn: aiosqlite.Connection, infohash: str):
    query = """
    SELECT * FROM tracker
    WHERE infohash = ?
    """
    async with conn.execute(query, (infohash,)) as cursor:
        row = await cursor.fetchone()
        if row:
            return TrackerEntry(
                infohash=row[0],
                validator_id=row[1],
                filename=row[2],
                length=row[3],
                chunk_length=row[4],
                chunk_count=row[5],
                chunk_hashes=json.loads(row[6]),
                creation_timestamp=row[7],
                signature=row[8],
            )
        return None  # Entry not found


async def delete_tracker_entry(conn: aiosqlite.Connection, infohash: str):
    query = """
    DELETE FROM tracker
    WHERE infohash = ?
    """
    await conn.execute(query, (infohash,))
    await conn.commit()


async def set_chunk_entry(conn: aiosqlite.Connection, entry: ChunkEntry):
    query = """
    INSERT INTO chunk (chunk_hash, infohash, validator_id, piece_hashes, chunk_idx, k, m, chunk_size, padlen, original_chunk_size, signature)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    await conn.execute(
        query,
        (
            entry.chunk_hash,
            entry.infohash,
            entry.validator_id,
            json.dumps(entry.piece_hashes),
            entry.chunk_idx,
            entry.k,
            entry.m,
            entry.chunk_size,
            entry.padlen,
            entry.original_chunk_size,
            entry.signature,
        ),
    )
    await conn.commit()


async def get_chunk_entry(conn: aiosqlite.Connection, chunk_hash: str):
    query = """
    SELECT * FROM chunk
    WHERE chunk_hash = ?
    """
    async with conn.execute(query, (chunk_hash,)) as cursor:
        row = await cursor.fetchone()
        if row:
            return ChunkEntry(
                chunk_hash=row[0],
                infohash=row[1],
                validator_id=row[2],
                piece_hashes=json.loads(row[3]),
                chunk_idx=row[4],
                k=row[5],
                m=row[6],
                chunk_size=row[7],
                padlen=row[8],
                original_chunk_size=row[9],
                signature=row[10],
            )
        return None  # Entry not found


async def delete_chunk_entry(conn: aiosqlite.Connection, chunk_hash: str):
    query = """
    DELETE FROM chunk
    WHERE chunk_hash = ?
    """
    await conn.execute(query, (chunk_hash,))
    await conn.commit()


async def set_piece_entry(conn: aiosqlite.Connection, entry: PieceEntry):
    query = """
    INSERT INTO piece (piece_hash, chunk_hash, miner_id, chunk_idx, piece_idx, piece_type, signature)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    await conn.execute(
        query,
        (
            entry.piece_hash,
            entry.chunk_hash,
            entry.miner_id,
            entry.chunk_idx,
            entry.piece_idx,
            entry.piece_type,
            entry.signature,
        ),
    )
    await conn.commit()


async def get_piece_entry(conn: aiosqlite.Connection, piece_hash: str):
    query = """
    SELECT * FROM piece
    WHERE piece_hash = ?
    """
    async with conn.execute(query, (piece_hash,)) as cursor:
        row = await cursor.fetchone()
        if row:
            return PieceEntry(
                piece_hash=row[0],
                chunk_hash=row[1],
                miner_id=row[2],
                chunk_idx=row[3],
                piece_idx=row[4],
                piece_type=row[5],
                signature=row[6],
            )
        return None  # Entry not found


async def delete_piece_entry(conn: aiosqlite.Connection, piece_hash: str):
    query = """
    DELETE FROM piece
    WHERE piece_hash = ?
    """
    await conn.execute(query, (piece_hash,))
    await conn.commit()
