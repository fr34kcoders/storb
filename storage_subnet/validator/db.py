"""
Provides functions for interacting with validator's database
"""

import json
from contextlib import asynccontextmanager
from typing import Optional

import aiosqlite

DB_DIR = "validator_database.db"
PIECE_ID_MINER_UIDS = "piece_id_miner_uids"
METADATA = "metadata"
INFOHASH_PIECE_IDS = "infohash_piece_ids"


@asynccontextmanager
async def get_db_connection(db_dir: str, uri: bool = False):  # noqa: ANN201
    conn = await aiosqlite.connect(db_dir, uri=uri)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
    try:
        yield conn  # Provide the connection to the calling code
    finally:
        await conn.close()  # Ensure the connection is properly closed


async def store_infohash_piece_ids(
    conn: aiosqlite.Connection, infohash: str, piece_ids: list[str]
) -> None:
    dict_piece_ids = {"piece_ids": piece_ids}
    query = f"INSERT INTO {INFOHASH_PIECE_IDS} VALUES (?, ?)"

    await conn.execute(
        query,
        (infohash, json.dumps(dict_piece_ids)),
    )
    await conn.commit()


async def store_metadata(
    conn: aiosqlite.Connection,
    infohash: str,
    filename: str,
    timestamp: str,
    piece_length: int,
    length: int,
) -> None:
    query = f"INSERT INTO {METADATA} VALUES (?, ?, ?, ?, ?)"

    await conn.execute(
        query,
        (
            infohash,
            filename,
            timestamp,
            piece_length,
            length,
        ),
    )

    await conn.commit()


async def get_metadata(
    conn: aiosqlite.Connection,
    infohash: str,
) -> dict:
    query = f"SELECT * FROM {METADATA} WHERE infohash = ?"

    # Use parameterized query to safely include infohash
    cur = await conn.execute(query, (infohash,))
    row = await cur.fetchone()

    if row is None:
        return None

    return dict(row)


# TODO: get stuff
async def get_pieces_from_infohash(
    conn: aiosqlite.Connection, infohash: str
) -> Optional[list[str]]:  # noqa: F821
    query = f"SELECT piece_ids FROM {INFOHASH_PIECE_IDS} WHERE infohash = ?"

    cur = await conn.execute(query, (infohash,))  # Use a tuple for parameters
    row = await cur.fetchone()

    if row is None:
        return None

    return json.loads(row["piece_ids"])  # Extract JSON from the result
