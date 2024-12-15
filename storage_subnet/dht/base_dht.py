import asyncio
import threading

import bittensor as bt
from kademlia.network import Server


class BaseDHT:
    def __init__(self, port: int = 6942):
        self.server = Server()
        self.port = port
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self.bootstrap_node = None
        self.is_running = threading.Event()  # To signal when the server is running

    def _run_server(self):
        """Run the DHT server in a separate thread."""
        asyncio.set_event_loop(self.loop)
        self.loop.set_debug(True)

        self.loop.run_until_complete(self.server.listen(self.port))

        if self.bootstrap_node:
            bt.logging.info(f"Bootstrapping DHT server to {self.bootstrap_node}")
            self.loop.run_until_complete(self.server.bootstrap([self.bootstrap_node]))

        bt.logging.info(f"DHT Server started and listening on port {self.port}")
        self.is_running.set()  # Signal that the server is ready
        try:
            self.loop.run_forever()

        except Exception as e:
            bt.logging.error(f"Error in DHT server loop: {e}")

    async def start(
        self, bootstrap_node_ip: str = None, bootstrap_node_port: int = None
    ):
        """Start the DHT server on a separate thread."""
        if self.thread and self.thread.is_alive():
            bt.logging.info("DHT Server is already running.")
            return

        self.bootstrap_node = (
            (bootstrap_node_ip, bootstrap_node_port) if bootstrap_node_ip else None
        )
        # Start the server thread
        self.thread = threading.Thread(target=self._run_server, daemon=True)
        self.thread.start()

        # Wait for the server to be ready
        if not self.is_running.wait(timeout=5):  # Wait up to 5 seconds
            bt.logging.error("DHT Server failed to start within the timeout period.")
            raise RuntimeError("DHT Server did not start.")

        bt.logging.info("DHT Server thread started.")

        await self.server.set("test", "test")

    async def stop(self):
        """Stop the DHT server and its thread."""
        if self.loop.is_running():
            bt.logging.info("Stopping DHT server...")
            self.loop.call_soon_threadsafe(self.loop.stop)

        if self.thread:
            self.thread.join(timeout=5)  # Wait for the thread to finish
            if self.thread.is_alive():
                bt.logging.error("DHT server thread did not shut down cleanly.")
            else:
                bt.logging.info("DHT server thread stopped successfully.")

        await self.server.stop()
        bt.logging.info("DHT Server stopped.")
