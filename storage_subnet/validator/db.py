"""
Provides functions for interacting with validator's database
"""

import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import aiosqlite

BALANCE = "balance"
KEY = "key"
NAME = "name"

RATE_LIMIT_PER_MINUTE = "rate_limit_per_minute"
API_KEYS_TABLE = "api_keys"
LOGS_TABLE = "logs"
ENDPOINT = "endpoint"
CREATED_AT = "created_at"

DB_DIR = "validator_database.db"
PIECE_ID_MINER_UIDS = "piece_id_miner_uids"
METADATA = "metadata"
INFOHASH_CHUNK_IDS = "infohash_chunk_ids"



@asynccontextmanager
async def get_db_connection(db_dir: str, uri: bool = False):  # noqa: ANN201
    conn = await aiosqlite.connect(db_dir, uri=uri)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints
    try:
        yield conn  # Provide the connection to the calling code
    finally:
        await conn.close()  # Ensure the connection is properly closed

async def get_api_key_info(conn: aiosqlite.Connection, api_key: str) -> dict | None:
    row = await (await conn.execute(f"SELECT * FROM {API_KEYS_TABLE} WHERE {KEY} = ?", (api_key,))).fetchone()
    return dict(row) if row else None


async def get_all_api_keys(conn: aiosqlite.Connection) -> list:
    return await (await conn.execute(f"SELECT * FROM {API_KEYS_TABLE}")).fetchall()


async def get_all_logs_for_key(conn: aiosqlite.Connection, api_key: str) -> list:
    return await (await conn.execute(f"SELECT * FROM {LOGS_TABLE} WHERE {KEY} = ?", (api_key,))).fetchall()


async def get_all_logs(conn: aiosqlite.Connection) -> list:
    return await (await conn.execute(f"SELECT * FROM {LOGS_TABLE}")).fetchall()


async def add_api_key(
    conn: aiosqlite.Connection,
    api_key: str,
    balance: float,
    rate_limit_per_minute: int,
    name: str,
) -> None:
    await conn.execute(
        f"INSERT INTO {API_KEYS_TABLE} VALUES (?, ?, ?, ?, ?)",
        (api_key, name, balance, rate_limit_per_minute, datetime.now()),  # noqa: DTZ005
    )
    await conn.commit()


async def update_api_key_balance(conn: aiosqlite.Connection, key: str, balance: float) -> None:
    await conn.execute(f"UPDATE {API_KEYS_TABLE} SET {BALANCE} = ? WHERE {KEY} = ?", (balance, key))
    await conn.commit()


async def update_api_key_rate_limit(conn: aiosqlite.Connection, key: str, rate: int) -> None:
    await conn.execute(
        f"UPDATE {API_KEYS_TABLE} SET {RATE_LIMIT_PER_MINUTE} = ? WHERE {KEY} = ?",
        (rate, key),
    )
    await conn.commit()


async def update_api_key_name(conn: aiosqlite.Connection, key: str, name: str) -> None:
    await conn.execute(f"UPDATE {API_KEYS_TABLE} SET {NAME} = ? WHERE {KEY} = ?", (name, key))
    await conn.commit()


async def delete_api_key(conn: aiosqlite.Connection, api_key: str) -> None:
    await conn.execute(f"DELETE FROM {API_KEYS_TABLE} WHERE {KEY} = ?", (api_key,))
    await conn.commit()


async def update_requests_and_credits(conn: aiosqlite.Connection, api_key_info: dict, cost: float) -> None:
    await conn.execute(
        f"UPDATE api_keys SET {BALANCE} = {BALANCE} - {cost} WHERE {KEY} = ?",
        (api_key_info[KEY],),
    )


async def log_request(conn: aiosqlite.Connection, api_key_info: dict, path: str, cost: float) -> None:
    info = await get_api_key_info(conn, api_key_info[KEY])
    if isinstance(info, dict):
        balance = info[BALANCE]

        await conn.execute(
            f"INSERT INTO {LOGS_TABLE} VALUES (?, ?, ?, ?, ?)",
            (info[KEY], path, cost, balance, datetime.now()),  # noqa: DTZ005
        )


async def rate_limit_exceeded(conn: aiosqlite.Connection, api_key_info: dict) -> bool:
    one_minute_ago = datetime.now() - timedelta(minutes=1)  # noqa: DTZ005

    # Prepare a SQL statement
    query = f"""
        SELECT *
        FROM logs
        WHERE {KEY} = ? AND {CREATED_AT} >= ?
    """

    cur = await conn.execute(query, (api_key_info[KEY], one_minute_ago.strftime("%Y-%m-%d %H:%M:%S")))
    recent_logs = await cur.fetchall()

    return len(recent_logs) >= api_key_info[RATE_LIMIT_PER_MINUTE]

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


async def store_infohash_chunk_ids(
    conn: aiosqlite.Connection, infohash: str, chunk_ids: list[str]
) -> None:
    dict_chunk_ids = {"chunk_ids": chunk_ids}
    query = f"INSERT INTO {INFOHASH_CHUNK_IDS} VALUES (?, ?)"

    await conn.execute(
        query,
        (infohash, json.dumps(dict_chunk_ids)),
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
async def get_chunks_from_infohash(
    conn: aiosqlite.Connection, infohash: str
) -> Optional[list[str]]:  # noqa: F821
    query = f"SELECT chunk_ids FROM {INFOHASH_CHUNK_IDS} WHERE infohash = ?"

    cur = await conn.execute(query, (infohash,))  # Use a tuple for parameters
    row = await cur.fetchone()

    if row is None:
        return None

    return json.loads(row["chunk_ids"])  # Extract JSON from the result
