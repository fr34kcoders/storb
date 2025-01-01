import asyncio
import logging as pylog
import sys
from abc import ABC, abstractmethod

import httpx
import uvicorn
from fastapi import FastAPI
from fiber.chain import chain_utils, interface, post_ip_to_chain
from fiber.chain.metagraph import Metagraph
from fiber.logging_utils import get_logger
from substrateinterface.keypair import Keypair

from storb import __spec_version__, get_spec_version
from storb.config import Config
from storb.constants import NeuronType
from storb.dht.base_dht import DHT
from storb.util.logging import setup_event_logger, setup_rotating_logger

logger = get_logger(__name__)

LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MiB
SYNC_FREQUENCY = 60  # 1 minute


class Neuron(ABC):
    """Base class for Bittensor neurons (miners and validators)"""

    def __init__(self, neuron_type: NeuronType = NeuronType.Base):
        self.config = Config(neuron_type)
        self.settings = self.config.settings
        self.spec_version = __spec_version__

        assert (
            get_spec_version(self.settings.version) == self.spec_version
        ), "The spec versions must match"

        # Miners and validators must set these themselves
        self.keypair: Keypair = None
        self.uid: str = None
        self.app: FastAPI = None
        self.server: uvicorn.Server = None
        self.httpx_client: httpx.AsyncClient = None
        self.api_port: int = self.settings.api_port

        assert self.settings.wallet_name, "Wallet name is not defined"
        assert self.settings.hotkey_name, "Hotkey name is not defined"

        logger.info(
            f"Attempt to load hotkey keypair with wallet name: {self.settings.wallet_name}"
        )
        self.keypair = chain_utils.load_hotkey_keypair(
            self.settings.wallet_name, self.settings.hotkey_name
        )

        self.netuid = self.settings.netuid
        self.external_ip = self.settings.external_ip

        self.subtensor_network = self.settings.subtensor.network
        self.subtensor_address = self.settings.subtensor.address

        self.substrate = interface.get_substrate(
            subtensor_network=self.subtensor_network,
            subtensor_address=self.subtensor_address,
        )

        if self.settings.post_ip:
            post_ip_to_chain.post_node_ip_to_chain(
                self.substrate,
                self.keypair,
                self.netuid,
                self.external_ip,
                self.api_port,
                self.keypair.ss58_address,
            )

        self.metagraph = Metagraph(netuid=self.netuid, substrate=self.substrate)
        self.metagraph.sync_nodes()

        self.dht = DHT(self.settings.dht.port)

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

        dht_bootstrap_ip = self.settings.dht.bootstrap.ip
        dht_bootstrap_port = self.settings.dht.bootstrap.port

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
