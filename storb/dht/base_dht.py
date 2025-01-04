import asyncio
import json
import pickle
import threading
from pathlib import Path

from fiber.logging_utils import get_logger
from kademlia.network import Server

from storb.constants import (
    DHT_QUERY_TIMEOUT,
    DHT_STARTUP_AND_SHUTDOWN_TIMEOUT,
)
from storb.dht.chunk_dht import ChunkDHTValue
from storb.dht.piece_dht import PieceDHTValue
from storb.dht.storage import PersistentStorageDHT, build_store_key
from storb.dht.tracker_dht import TrackerDHTValue

logger = get_logger(__name__)


class DHT:
    def __init__(
        self,
        file: str,
        db: str,
        port: int,
        startup_timeout: int = DHT_STARTUP_AND_SHUTDOWN_TIMEOUT,
    ):
        """Initialize the DHT server.

        Parameters
        ----------
        file: str
            Path to the save file to load the DHT state from.
        db: str
            Path to the database file to store the DHT data.
        port : int (optional)
            Port number to run the DHT server on.
        startup_timeout : int (optional)
            Timeout in seconds to wait for the DHT server to start. Defaults to 5 seconds.
        """

        self.server: Server = Server(
            storage=PersistentStorageDHT(
                db_dir=db,
            ),
        )
        self.port: int = port
        self.file: str = file
        self.db: str = db
        self.startup_timeout: int = startup_timeout
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self.thread: threading.Thread | None = None
        self.bootstrap_node: tuple[str, int] | None = None
        self.neighbor_nodes: list[
            tuple[str, int]
        ] = []  # Used when loading the state from the save file
        self.is_running: threading.Event = threading.Event()

    def _setup_server(self):
        """Run the DHT server in the event loop.

        Raises
        ------
        RuntimeError
            If the server fails to start.

        Notes
        -----
        If there is a save file, the server will load the state from the save file.
        If there is no save file, the server will start listening on the port.
        """

        asyncio.set_event_loop(self.loop)

        # Load the state from the save file if it exists
        file_path = Path(self.file)
        if file_path.exists():
            logger.debug(f"Loading DHT state from {self.file}")
            try:
                with open(self.file, "rb") as f:
                    logger.info("Loading state from %s", self.file)
                    data = pickle.load(f)
                    self.server = Server(
                        storage=PersistentStorageDHT(
                            db_dir=self.db,
                        ),
                        ksize=data["ksize"],
                        alpha=data["alpha"],
                        node_id=data["id"],
                    )

                    if data["neighbors"]:
                        logger.info(
                            f"Bootstrapping DHT server with neighbors, {data['neighbors']}"
                        )
                        # Check if the neighbors are still alive
                        for neighbor in data["neighbors"]:
                            logger.info(f"Adding neighbor {neighbor}")
                            self.neighbor_nodes.append(neighbor)
                    logger.info("DHT state loaded successfully.")
            except Exception as e:
                logger.error(f"Failed to load DHT state from {self.file}: {e}")
                raise RuntimeError(f"Failed to load DHT state from {self.file}") from e

        try:
            self.loop.run_until_complete(self.server.listen(self.port))
            self.server.save_state_regularly(self.file, 10)
            for neighbor in self.neighbor_nodes:
                self.loop.run_until_complete(self.server.bootstrap([neighbor]))
        except Exception as e:
            logger.error(f"Failed to start server on port {self.port}: {e}")
            raise RuntimeError(f"Server failed to start on port {self.port}") from e

    def _bootstrap_server(self):
        """Bootstrap the DHT server to the known node."""
        if self.bootstrap_node:
            logger.info(f"Bootstrapping DHT server to {self.bootstrap_node}")
            self.loop.run_until_complete(self.server.bootstrap([self.bootstrap_node]))

    def _run_server(self):
        """Run the DHT server in a separate thread."""
        self._setup_server()
        self._bootstrap_server()
        logger.info(f"DHT Server started and listening on port {self.port}")
        self.is_running.set()
        try:
            self.loop.run_forever()
        except asyncio.CancelledError:
            logger.info("DHT server loop cancelled.")
        except Exception as e:
            logger.error(f"Unexpected error in DHT server loop: {e}")
        finally:
            # Ensure all background tasks are stopped before exiting the loop
            pending_tasks = asyncio.all_tasks(self.loop)
            if pending_tasks:
                logger.info("Stopping all pending tasks...")
                for task in pending_tasks:
                    task.cancel()
                asyncio.run_coroutine_threadsafe(
                    asyncio.gather(*pending_tasks, return_exceptions=True), self.loop
                ).result()

            self.loop.close()
            logger.info("DHT server event loop closed.")
            self.is_running.clear()

    async def start(
        self, bootstrap_node_ip: str = None, bootstrap_node_port: int = None
    ):
        """Start the DHT server and its thread.

        Parameters
        ----------
        bootstrap_node_ip : str (optional)
            IP address of the bootstrap node to connect to.
        bootstrap_node_port : int (optional)
            Port number of the bootstrap node to connect to.

        Raises
        ------
        ValueError
            If the bootstrap_node_port is not a positive integer.
        """

        if self.thread and self.thread.is_alive():
            logger.info("DHT Server is already running.")
            return

        if bootstrap_node_ip and bootstrap_node_port:
            if not isinstance(bootstrap_node_port, int) or bootstrap_node_port <= 0:
                raise ValueError(
                    "Invalid bootstrap_node_port. Must be a positive integer."
                )
            self.bootstrap_node = (bootstrap_node_ip, bootstrap_node_port)
        else:
            self.bootstrap_node = None

        # Start the server thread
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()

        logger.info("DHT Server thread started.")

    async def stop(self):
        """Stop the DHT server and its thread."""
        if self.loop.is_running():
            logger.info("Stopping DHT server...")
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=DHT_STARTUP_AND_SHUTDOWN_TIMEOUT)
            if self.thread.is_alive():
                logger.error("DHT server thread did not shut down cleanly.")
            else:
                logger.info("DHT server thread stopped successfully.")

        await self.server.stop()

        # Ensure all pending tasks are completed before closing the loop
        pending_tasks = asyncio.all_tasks(self.loop)
        if pending_tasks:
            logger.info("Waiting for pending tasks to complete...")
            for task in pending_tasks:
                task.cancel()
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        self.loop.close()
        logger.info("DHT Server stopped and event loop closed.")

    # Tracker DHT methods
    async def store_tracker_entry(self, infohash: str, value: TrackerDHTValue) -> None:
        """Store a tracker entry in the DHT.

        Parameters
        ----------
        infohash : str
            Infohash of the torrent to store the tracker entry for.
        value : TrackerDHTValue
            TrackerDHTValue object to store in the DHT.

        Raises
        ------
        RuntimeError
            If the tracker entry fails to store.

        Notes
        -----
        The key for the tracker entry is "tracker:infohash".
        """

        logger.info(f"Storing tracker entry for infohash: {infohash}")
        value: bytes = value.model_dump_json().encode("utf-8")
        key: bytes = build_store_key("tracker", infohash)
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(key, value),
                self.loop,
            )
            res = future.result(timeout=DHT_QUERY_TIMEOUT)
            if not res:
                raise RuntimeError(f"Failed to store tracker entry for {infohash}")
            logger.info(f"Successfully stored tracker entry for {infohash}")
        except Exception as e:
            raise RuntimeError(f"Failed to store tracker entry for {infohash}: {e}")

    async def get_tracker_entry(self, infohash: str) -> TrackerDHTValue:
        """Retrieve a tracker entry from the DHT.

        Parameters
        ----------
        infohash : str
            Infohash of the file to retrieve the tracker entry for.

        Returns
        -------
        TrackerDHTValue
            TrackerDHTValue object for the infohash.

        Raises
        ------
        RuntimeError
            If the tracker entry fails to retrieve.
        """

        logger.info(f"Retrieving tracker entry for infohash: {infohash}")
        key = build_store_key("tracker", infohash)
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(self.server.get(key), self.loop)
            value: bytes = future.result(timeout=DHT_QUERY_TIMEOUT)
            if not value:
                return None
            logger.info(f"Successfully retrieved tracker entry for {infohash}")
            value.decode("utf-8")
            value = json.loads(value)
            value = TrackerDHTValue(**value)
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}: {e}")

    # Chunk DHT methods
    async def store_chunk_entry(self, chunk_hash: str, value: ChunkDHTValue) -> None:
        """Store a chunk entry in the DHT.

        Parameters
        ----------
        chunk_hash : str
            Chunk hash of the chunk to store the chunk entry for.
        value : ChunkDHTValue
            ChunkDHTValue object to store in the DHT.

        Raises
        ------
        RuntimeError
            If the chunk entry fails to store.

        Notes
        -----
        The key for the chunk entry is "chunk:chunk_hash".
        """

        logger.info(f"Storing chunk entry for chunk_hash: {chunk_hash}")
        value: bytes = value.model_dump_json().encode("utf-8")
        key: bytes = build_store_key("chunk", chunk_hash)
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(key, value),
                self.loop,
            )
            res = future.result(timeout=DHT_QUERY_TIMEOUT)
            if not res:
                raise RuntimeError(f"Failed to store chunk entry for {chunk_hash}")
            logger.info(f"Successfully stored chunk entry for {chunk_hash}")
        except Exception as e:
            raise RuntimeError(f"Failed to store chunk entry for {chunk_hash}: {e}")

    async def get_chunk_entry(self, chunk_hash: str) -> ChunkDHTValue:
        """Retrieve a chunk entry from the DHT.

        Parameters
        ----------
        chunk_hash : str
            Chunk hash of the chunk to retrieve the chunk entry for.

        Returns
        -------
        ChunkDHTValue
            ChunkDHTValue object for the chunk_hash.

        Raises
        ------
        RuntimeError
            If the chunk entry fails to retrieve.
        """
        logger.info(f"Retrieving chunk entry for chunk hash: {chunk_hash}")
        key = build_store_key("chunk", chunk_hash)
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(self.server.get(key), self.loop)
            value: bytes = future.result(timeout=DHT_QUERY_TIMEOUT)
            if not value:
                return None
            logger.info(f"Successfully retrieved chunk entry for {chunk_hash}")
            value.decode("utf-8")
            value = json.loads(value)
            value = ChunkDHTValue(**value)
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve chunk entry for {chunk_hash}: {e}")

    # Piece DHT methods
    async def store_piece_entry(self, piece_hash: str, value: PieceDHTValue) -> None:
        """Store a piece entry in the DHT.

        Parameters
        ----------
        piece_hash : str
            Piece hash of the piece to store the piece entry for.
        value : PieceDHTValue
            PieceDHTValue object to store in the DHT.

        Raises
        ------
        RuntimeError
            If the piece entry fails to store.

        Notes
        -----
        The key for the piece entry is "piece:piece_hash".
        """
        logger.info(f"Storing piece entry for piece_hash: {piece_hash}")
        value: bytes = value.model_dump_json().encode("utf-8")
        key: bytes = build_store_key("piece", piece_hash)
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(key, value),
                self.loop,
            )
            res = future.result(timeout=DHT_QUERY_TIMEOUT)
            if not res:
                raise RuntimeError(f"Failed to store piece entry for {piece_hash}")
            logger.info(f"Successfully stored piece entry for {piece_hash}")
        except Exception as e:
            raise RuntimeError(f"Failed to store piece entry for {piece_hash}: {e}")

    # Piece DHT methods
    async def get_piece_entry(self, piece_hash: str) -> PieceDHTValue:
        """Retrieve a piece entry from the DHT.

        Parameters
        ----------
        piece_hash : str
            Piece hash of the piece to retrieve the piece entry for.

        Returns
        -------
        PieceDHTValue
            PieceDHTValue object for the piece_hash.

        Raises
        ------
        RuntimeError
            If the piece entry fails to retrieve.
        """
        logger.info(f"Retrieving piece entry for piece hash: {piece_hash}")
        key = build_store_key("piece", piece_hash)
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(self.server.get(key), self.loop)
            value: bytes = future.result(timeout=DHT_QUERY_TIMEOUT)
            if not value:
                return None
            logger.info(f"Successfully retrieved piece entry for {piece_hash}")
            value.decode("utf-8")
            value = json.loads(value)
            value = PieceDHTValue(**value)
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve piece entry for {piece_hash}: {e}")

    def __enter__(self):
        asyncio.run(self.start())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.stop())
