"""
Provides functions for interacting with validator's database
"""

import json
from contextlib import asynccontextmanager
from enum import StrEnum

import aiosqlite
from pydantic import BaseModel

from storb.util.piece import PieceType


class DHTValueNamespace(StrEnum):
    CHUNK = "chunk"
    PIECE = "piece"
    TRACKER = "tracker"


class ChunkEntry(BaseModel):
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


class PieceEntry(BaseModel):
    piece_hash: str
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
    chunk_size: int
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
    INSERT INTO tracker (infohash, validator_id, filename, length, chunk_size, chunk_count, chunk_hashes, creation_timestamp, signature)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(infohash)
    DO UPDATE SET
    validator_id = excluded.validator_id,
    filename = excluded.filename,
    length = excluded.length,
    chunk_size = excluded.chunk_size,
    chunk_count = excluded.chunk_count,
    chunk_hashes = excluded.chunk_hashes,
    creation_timestamp = excluded.creation_timestamp,
    signature = excluded.signature
    """
    await conn.execute(
        query,
        (
            entry.infohash,
            entry.validator_id,
            entry.filename,
            entry.length,
            entry.chunk_size,
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
                chunk_size=row[4],
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
    await conn.execute(query, (infohash))
    await conn.commit()


async def set_chunk_entry(conn: aiosqlite.Connection, entry: ChunkEntry):
    query = """
    INSERT INTO chunk (chunk_hash, validator_id, piece_hashes, chunk_idx, k, m, chunk_size, padlen, original_chunk_size, signature)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(chunk_hash)
    DO UPDATE SET
    validator_id = excluded.validator_id,
    piece_hashes = excluded.piece_hashes,
    chunk_idx = excluded.chunk_idx,
    k = excluded.k,
    m = excluded.m,
    chunk_size = excluded.chunk_size,
    padlen = excluded.padlen,
    original_chunk_size = excluded.original_chunk_size,
    signature = excluded.signature
    """
    await conn.execute(
        query,
        (
            entry.chunk_hash,
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
                validator_id=row[1],
                piece_hashes=json.loads(row[2]),
                chunk_idx=row[3],
                k=row[4],
                m=row[5],
                chunk_size=row[6],
                padlen=row[7],
                original_chunk_size=row[8],
                signature=row[9],
            )
        return None


async def delete_chunk_entry(conn: aiosqlite.Connection, chunk_hash: str):
    query = """
    DELETE FROM chunk
    WHERE chunk_hash = ?
    """
    await conn.execute(query, (chunk_hash,))
    await conn.commit()


async def set_piece_entry(conn: aiosqlite.Connection, entry: PieceEntry):
    query = """
    INSERT INTO piece (piece_hash, miner_id, chunk_idx, piece_idx, piece_type, signature)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(piece_hash)
    DO UPDATE SET
    miner_id = excluded.miner_id,
    chunk_idx = excluded.chunk_idx,
    piece_idx = excluded.piece_idx,
    piece_type = excluded.piece_type,
    signature = excluded.signature
    """
    await conn.execute(
        query,
        (
            entry.piece_hash,
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
                miner_id=row[1],
                chunk_idx=row[2],
                piece_idx=row[3],
                piece_type=row[4],
                signature=row[5],
            )
        return None  # Entry not found


async def delete_piece_entry(conn: aiosqlite.Connection, piece_hash: str):
    query = """
    DELETE FROM piece
    WHERE piece_hash = ?
    """
    await conn.execute(query, (piece_hash,))
    await conn.commit()
