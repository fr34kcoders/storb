import asyncio
import threading
import time
from typing import Union

from fiber.logging_utils import get_logger
from kademlia.storage import IStorage

from storb import db
from storb.dht.chunk_dht import ChunkDHTValue
from storb.dht.piece_dht import PieceDHTValue
from storb.dht.tracker_dht import TrackerDHTValue

logger = get_logger(__name__)

DHTValue = Union[ChunkDHTValue, PieceDHTValue, TrackerDHTValue]


def build_store_key(namespace: str, key: str) -> bytes:
    """Build a key for the DHT storage.

    Parameters
    ----------
    namespace : str
        namespace of the key (chunk, piece, tracker)
    key : str
        key to store

    Returns
    -------
    store_key : bytes
        The key in 'namespace:key' format encoded in UTF-8.
    """

    return f"{namespace}:{key}".encode("utf-8")


def parse_store_key(store_key: bytes) -> tuple[str, str]:
    """Parse a key from the DHT storage.

    Parameters
    ----------
    store_key : bytes
        The key in 'namespace:key' format encoded in UTF-8.

    Returns
    -------
    [namespace, key] : tuple[str, str]
        The namespace and key.
    """

    return store_key.decode("utf-8").split(":", 1)


class PersistentStorageDHT(IStorage):
    """Persistent storage for the DHT.

    This class stores DHT values in memory and in a SQLite database.
    """

    def __init__(self, db_dir: str):
        """Initialize the PersistentStorageDHT.

        Parameters
        ----------
            db_dir (str, optional): The path to the database directory.
        """

        self.db_path = db_dir
        self.mem: dict[bytes, tuple[DHTValue, float]] = {}

        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._start_event_loop, daemon=True)
        self.thread.start()

    def _start_event_loop(self):
        """Start the event loop for the DB."""

        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def set(self, key: bytes, value: bytes):
        """Store a value in the DHT.

        Parameters
        ----------
        key : bytes
            The key in 'namespace:key' format encoded in UTF-8.
        value : DHTValue
            The value to store.
        """

        self.mem[key] = (value, time.time())
        namespace, key = parse_store_key(key)
        # Asynchronously write to the database
        future = asyncio.run_coroutine_threadsafe(
            self._db_write_data(namespace, key, value), self.loop
        )
        try:
            future.result()
        except Exception as e:
            logger.error(f"Database write error for key {key}: {e}")
            raise

    def __getitem__(self, key: bytes) -> bytes:
        """Retrieve a value from the DHT corresponding to the key.

        Parameters
        ----------
        key : bytes
            The key in 'namespace:key' format encoded in UTF-8.

        Returns
        -------
        value : DHTValue
            The retrieved value.

        Raises
        ------
        KeyError
            If the key does not exist.

        ValueError
            If the key format is invalid.
        """

        logger.debug(f"Getting {key}")
        # Check in-memory cache
        if key in self.mem:
            value, _ = self.mem[key]
            logger.debug(f"Retrieved from memory: {value}")
            return value

        # Retrieve from database
        future = asyncio.run_coroutine_threadsafe(self._db_read_data(key), self.loop)
        try:
            value = future.result()
            if value is None:
                logger.debug(f"No entry found for key {key}")
                return None
            self.mem[key] = (value, time.time())
            logger.debug(f"Retrieved from DB: {key}")
            return value
        except Exception as e:
            logger.error(f"Database read error for key {key}: {e}")
            raise

    def get(self, key: bytes, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def iter_older_than(self, seconds_old: float):
        """Iterate over values older than a certain time.

        Parameters
        ----------
        seconds_old : float
            The time in seconds.

        Returns
        -------
        results : list[tuple[bytes, DHTValue]]
            A list of tuples of the key and value.

        Notes
        -----
        This only iterates over in-memory data.
        """

        cutoff = time.time() - seconds_old
        results = []
        for k, (v, ts) in self.mem.items():
            if ts < cutoff:
                results.append((k, v))
        return results

    def __iter__(self):
        """Iterate over the keys and values in the DHT.

        Yields:
            tuple[bytes, DHTValue]: The key and value.
        """

        for k, (v, _) in self.mem.items():
            yield (k, v)

    async def _db_write_data(self, namespace: str, key: str, value: bytes):
        """Write to the DB in the event loop thread, based on the namespace.

        Parameters
        ----------
        namespace : str
            namespace of the key (chunk, piece, tracker)
        key : str
            key to store
        value : bytes
            value to store

        Raises
        ------
        ValueError
            If the value format is invalid.
        """

        try:
            value = value.decode("utf-8")
        except ValueError:
            raise ValueError(
                f"DISK: Invalid value format: {value}, expected utf-8 encoded bytes"
            )

        logger.debug(f"DISK: Setting {namespace}:{key} => {value}")
        async with db.get_db_connection(self.db_path) as conn:
            match namespace:
                case "chunk":
                    val = ChunkDHTValue.model_validate_json(value)
                    entry = db.ChunkEntry(
                        chunk_hash=key,
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
                    logger.info(f"flushing chunk entry {entry} to disk")
                    await db.set_chunk_entry(conn, entry)

                case "piece":
                    val = PieceDHTValue.model_validate_json(value)
                    entry = db.PieceEntry(
                        piece_hash=key,
                        miner_id=val.miner_id,
                        chunk_idx=val.chunk_idx,
                        piece_idx=val.piece_idx,
                        piece_type=val.piece_type,
                        signature=val.signature,
                    )
                    logger.info(f"flushing piece entry {entry} to disk")
                    await db.set_piece_entry(conn, entry)

                case "tracker":
                    val = TrackerDHTValue.model_validate_json(value)
                    entry = db.TrackerEntry(
                        infohash=key,
                        validator_id=val.validator_id,
                        filename=val.filename,
                        length=val.length,
                        chunk_size=val.chunk_size,
                        chunk_count=val.chunk_count,
                        chunk_hashes=val.chunk_hashes,
                        creation_timestamp=val.creation_timestamp,
                        signature=val.signature,
                    )
                    logger.info(f"flushing tracker entry {entry} to disk")
                    await db.set_tracker_entry(conn, entry)

                case _:
                    raise ValueError(f"Invalid key namespace: {namespace}")

    async def _db_read_data(self, key: bytes) -> DHTValue:
        """Read from the DB in the event loop thread, based on the namespace.

        Parameters
        ----------
        key : bytes
            The key in 'namespace:key' format encoded in UTF-8.

        Returns
        -------
        DHTValue
            The retrieved value.

        Raises
        ------
        ValueError
            If the key format is invalid.
        """

        logger.debug(f"Trying read for {key} from disk")
        try:
            namespace, db_key = parse_store_key(key)
        except ValueError:
            raise ValueError(f"Invalid key format: {key}")

        logger.debug(f"DISK: DB read for {namespace} : {db_key}")

        async with db.get_db_connection(self.db_path) as conn:
            match namespace:
                case "chunk":
                    logger.debug(f"DISK: DB read for chunk: {db_key}")
                    entry = await db.get_chunk_entry(conn, db_key)
                    if entry is None:
                        return None
                    return (
                        ChunkDHTValue(
                            chunk_hash=entry.chunk_hash,
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
                        .model_dump_json()
                        .encode("utf-8")
                    )

                case "piece":
                    logger.debug(f"DB read for piece: {db_key}")
                    entry = await db.get_piece_entry(conn, db_key)
                    if entry is None:
                        return None
                    return (
                        PieceDHTValue(
                            piece_hash=entry.piece_hash,
                            miner_id=entry.miner_id,
                            chunk_idx=entry.chunk_idx,
                            piece_idx=entry.piece_idx,
                            piece_type=entry.piece_type,
                            signature=entry.signature,
                        )
                        .model_dump_json()
                        .encode("utf-8")
                    )

                case "tracker":
                    logger.debug(f"DB read for tracker: {db_key}")
                    entry = await db.get_tracker_entry(conn, db_key)
                    if entry is None:
                        return None
                    return (
                        TrackerDHTValue(
                            infohash=entry.infohash,
                            validator_id=entry.validator_id,
                            filename=entry.filename,
                            length=entry.length,
                            chunk_size=entry.chunk_size,
                            chunk_count=entry.chunk_count,
                            chunk_hashes=entry.chunk_hashes,
                            creation_timestamp=entry.creation_timestamp,
                            signature=entry.signature,
                        )
                        .model_dump_json()
                        .encode("utf-8")
                    )

                case _:
                    raise ValueError(f"Invalid key {db_key} namespace: {namespace}")
