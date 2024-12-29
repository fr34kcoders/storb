import asyncio
import logging as pylog
import sys
from abc import ABC, abstractmethod

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fiber.chain import interface
from fiber.chain.metagraph import Metagraph
from fiber.logging_utils import get_logger
from substrateinterface.keypair import Keypair

from storb import __spec_version__
from storb.constants import DHT_PORT
from storb.dht.base_dht import DHT
from storb.util.config import Config
from storb.util.logging import setup_event_logger, setup_rotating_logger

logger = get_logger(__name__)

LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MiB
SYNC_FREQUENCY = 60  # 1 minute


class Neuron(ABC):
    """Base class for Bittensor neurons (miners and validators)"""

    def __init__(self):
        load_dotenv()

        self.config = Config()
        self.spec_version = __spec_version__

        # Miners and validators must set these themselves
        self.keypair: Keypair = None
        self.uid: str = None
        self.app: FastAPI = None
        self.server: uvicorn.Server = None
        self.httpx_client: httpx.AsyncClient = None
        self.api_port: int = 0

        self.wallet_name = self.config.T.wallet_name
        self.hotkey_name = self.config.T.hotkey_name

        self.netuid = self.config.T.netuid

        self.subtensor_network = self.config.T.subtensor_network
        self.subtensor_address = self.config.T.subtensor_address

        self.substrate = interface.get_substrate(
            subtensor_network=self.subtensor_network,
            subtensor_address=self.subtensor_address,
        )

        self.metagraph = Metagraph(netuid=self.netuid, substrate=self.substrate)
        self.metagraph.sync_nodes()

        self.dht = DHT(self.config.T.dht_port)

        setup_rotating_logger(
            logger_name="kademlia", log_level=pylog.DEBUG, max_size=LOG_MAX_SIZE
        )

        setup_event_logger(retention_size=LOG_MAX_SIZE)

    @abstractmethod
    async def start(self): ...

    @abstractmethod
    async def stop(self): ...

    async def start_dht(self):
        """Start the DHT server"""

        dht_bootstrap_ip = self.config.T.dht_bootstrap_ip
        dht_bootstrap_port = self.config.T.dht_bootstrap_port

        try:
            if dht_bootstrap_ip and dht_bootstrap_port:
                logger.info(
                    f"Starting DHT server on port {dht_bootstrap_port} with bootstrap node {dht_bootstrap_ip}"
                )
                await self.dht.start(dht_bootstrap_ip, dht_bootstrap_port)
            else:
                logger.info(f"Starting DHT server on port {self.dht.port}")
                await self.dht.start()
        except Exception as e:
            logger.error(f"Failed to start DHT server: {e}")
            raise RuntimeError("Failed to start DHT server") from e

        logger.info(f"DHT server started on port {self.dht.port}")

    def check_registration(self):
        if (
            not self.metagraph.nodes.get(self.keypair.ss58_address)
            or not self.metagraph.nodes.get(self.keypair.ss58_address).node_id
        ):
            logger.error(
                f"Wallet is not registered on netuid {self.netuid}."
                f" Please register the hotkey using `btcli subnets register` before trying again"
            )
            sys.exit(1)

    async def sync_metagraph(self):
        """Synchronize local metagraph state with chain.

        Creates new metagraph instance if needed and syncs node data.

        Raises
        ------
        Exception
            If metagraph sync fails
        """

        try:
            self.substrate = interface.get_substrate(
                subtensor_address=self.substrate.url
            )
            self.metagraph.sync_nodes()

            logger.info("Metagraph synced successfully")
        except Exception as e:
            logger.error(f"Failed to sync metagraph: {str(e)}")

    async def sync_loop(self):
        """Background task to sync metagraph"""

        while True:
            try:
                await self.sync_metagraph()
                await asyncio.sleep(SYNC_FREQUENCY)
            except Exception as e:
                logger.error(f"Error in sync metagraph: {e}")
                await asyncio.sleep(SYNC_FREQUENCY // 2)
