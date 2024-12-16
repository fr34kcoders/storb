import asyncio
import threading

import bittensor as bt
from kademlia.network import Server


class BaseDHT:
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
            bt.logging.error(f"Failed to start server on port {self.port}: {e}")
            raise RuntimeError(f"Server failed to start on port {self.port}") from e

    def _bootstrap_server(self):
        if self.bootstrap_node:
            bt.logging.info(f"Bootstrapping DHT server to {self.bootstrap_node}")
            self.loop.run_until_complete(self.server.bootstrap([self.bootstrap_node]))

    def _run_server(self):
        """Run the DHT server in a separate thread."""
        self._setup_server()
        self._bootstrap_server()
        bt.logging.info(f"DHT Server started and listening on port {self.port}")
        self.is_running.set()
        try:
            self.loop.run_forever()
        except asyncio.CancelledError:
            bt.logging.info("DHT server loop cancelled.")
        except Exception as e:
            bt.logging.error(f"Unexpected error in DHT server loop: {e}")
        finally:
            # Ensure all background tasks are stopped before exiting the loop
            pending_tasks = asyncio.all_tasks(self.loop)
            if pending_tasks:
                bt.logging.info("Stopping all pending tasks...")
                for task in pending_tasks:
                    task.cancel()
                asyncio.run_coroutine_threadsafe(
                    asyncio.gather(*pending_tasks, return_exceptions=True), self.loop
                ).result()

            self.loop.close()
            bt.logging.info("DHT server event loop closed.")
            self.is_running.clear()

    async def start(
        self, bootstrap_node_ip: str = None, bootstrap_node_port: int = None
    ):
        """Start the DHT server on a separate thread."""
        if self.thread and self.thread.is_alive():
            bt.logging.info("DHT Server is already running.")
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
            bt.logging.error("DHT Server failed to start within the timeout period.")
            raise RuntimeError("DHT Server did not start.")

        bt.logging.info("DHT Server thread started.")

    async def stop(self):
        """Stop the DHT server and its thread."""
        if self.loop.is_running():
            bt.logging.info("Stopping DHT server...")
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
            if self.thread.is_alive():
                bt.logging.error("DHT server thread did not shut down cleanly.")
            else:
                bt.logging.info("DHT server thread stopped successfully.")

        await self.server.stop()

        # Ensure all pending tasks are completed before closing the loop
        pending_tasks = asyncio.all_tasks(self.loop)
        if pending_tasks:
            bt.logging.info("Waiting for pending tasks to complete...")
            for task in pending_tasks:
                task.cancel()
            await asyncio.gather(*pending_tasks, return_exceptions=True)

        self.loop.close()
        bt.logging.info("DHT Server stopped and event loop closed.")

    def __enter__(self):
        asyncio.run(self.start())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        asyncio.run(self.stop())
