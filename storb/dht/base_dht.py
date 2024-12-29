import asyncio
import threading

from fiber.logging_utils import get_logger
from kademlia.network import Server

from storb.dht.piece_dht import ChunkDHTValue, PieceDHTValue
from storb.dht.tracker_dht import TrackerDHTValue

logger = get_logger(__name__)


class DHT:
    def __init__(self, port: int = 6942, startup_timeout: int = 5):
        self.server: Server = Server()
        self.port: int = port
        self.startup_timeout: int = startup_timeout
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self.thread: threading.Thread | None = None
        self.bootstrap_node: tuple[str, int] | None = None
        self.is_running: threading.Event = threading.Event()

    def _setup_server(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.server.listen(self.port))
        except Exception as e:
            logger.error(f"Failed to start server on port {self.port}: {e}")
            raise RuntimeError(f"Server failed to start on port {self.port}") from e

    def _bootstrap_server(self):
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
        """
        Start the DHT server on a separate thread.

        Parameters
        ----------
        bootstrap_node_ip : str
            Node IP for bootstrapping the DHT server.
        bootstrap_node_port : int
            Node port for bootstrapping the DHT server.
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

        # Wait for the server to be ready
        if not self.is_running.wait(timeout=self.startup_timeout):
            logger.error("DHT Server failed to start within the timeout period.")
            raise RuntimeError("DHT Server did not start.")

        logger.info("DHT Server thread started.")

    async def stop(self):
        """Stop the DHT server and its thread."""
        if self.loop.is_running():
            logger.info("Stopping DHT server...")
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
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
        """
        Store a tracker entry in the DHT.
        tracker:infohash -> TrackerDHTValue
        """
        logger.info(f"Storing tracker entry for infohash: {infohash}")
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
            logger.info(f"Successfully stored tracker entry for {infohash}")
        except Exception as e:
            raise RuntimeError(f"Failed to store tracker entry for {infohash}: {e}")

    async def get_tracker_entry(self, infohash: str) -> TrackerDHTValue:
        """
        Retrieve a tracker entry from the DHT.
        """
        logger.info(f"Retrieving tracker entry for infohash: {infohash}")
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.get(f"tracker:{infohash}"), self.loop
            )
            ser_value = future.result(timeout=5)
            if not ser_value:
                raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}")
            value = TrackerDHTValue.model_validate_json(ser_value)
            logger.info(f"Successfully retrieved tracker entry for {infohash}")
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve tracker entry for {infohash}: {e}")

    # Chunk DHT methods
    async def store_chunk_entry(self, chunk_id: str, value: ChunkDHTValue) -> None:
        """
        Store a chunk entry in the DHT.
        chunk:chunk_id -> ChunkDHTValue
        """
        logger.info(f"Storing chunk entry for chunk_id: {chunk_id}")
        ser_value = value.model_dump_json()
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.set(f"chunk:{chunk_id}", ser_value),
                self.loop,
            )
            res = future.result(timeout=5)
            if not res:
                raise RuntimeError(f"Failed to store chunk entry for {chunk_id}")
            logger.info(f"Successfully stored chunk entry for {chunk_id}")
        except Exception as e:
            raise RuntimeError(f"Failed to store chunk entry for {chunk_id}: {e}")

    async def get_chunk_entry(self, chunk_id: str) -> ChunkDHTValue:
        """
        Retrieve a chunk entry from the DHT.
        """
        logger.info(f"Retrieving chunk entry for chunk_id: {chunk_id}")
        try:
            if self.server.protocol.router is None:
                raise RuntimeError("Event loop is not set yet!")
            future = asyncio.run_coroutine_threadsafe(
                self.server.get(f"chunk:{chunk_id}"), self.loop
            )
            ser_value = future.result(timeout=5)
            if ser_value is None:
                raise RuntimeError(f"Failed to retrieve chunk entry for {chunk_id}")
            value = ChunkDHTValue.model_validate_json(ser_value)
            logger.info(f"Successfully retrieved chunk entry for {chunk_id}")
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve chunk entry for {chunk_id}: {e}")

    # Piece DHT methods
    async def store_piece_entry(self, piece_id: str, value: PieceDHTValue) -> None:
        """
        Store a piece entry in the DHT.
        piece:piece_id -> PieceDHTValue
        """
        logger.info(f"Storing piece entry for piece_id: {piece_id}")
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
            logger.info(f"Successfully stored piece entry for {piece_id}")
        except Exception as e:
            raise RuntimeError(f"Failed to store piece entry for {piece_id}: {e}")

    # Piece DHT methods
    async def get_piece_entry(self, piece_id: str) -> PieceDHTValue:
        """
        Retrieve a piece entry from the DHT.
        """
        logger.info(f"Retrieving piece entry for piece_id: {piece_id}")
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
            logger.info(f"Successfully retrieved piece entry for {piece_id}")
            return value
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve piece entry for {piece_id}: {e}")

    def __enter__(self):
        asyncio.run(self.start())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.stop())
