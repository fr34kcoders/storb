import logging as pylog
import sys
import time
from abc import ABC, abstractmethod

import httpx
import uvicorn
from fastapi import FastAPI
from fiber.chain import chain_utils, interface, post_ip_to_chain
from fiber.chain.metagraph import Metagraph
from fiber.logging_utils import get_logger

from storb import __spec_version__, get_spec_version
from storb.config import Config
from storb.constants import NeuronType
from storb.dht.base_dht import DHT
from storb.util.logging import setup_event_logger, setup_rotating_logger

logger = get_logger(__name__)

LOG_MAX_SIZE = 5 * 1024 * 1024  # 5 MiB


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
        self.app: FastAPI = None

        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.settings.api_port)
        self.server = uvicorn.Server(config)
        self.httpx_client = httpx.AsyncClient()
        assert self.server, "Uvicorn server must be initialised"
        assert self.httpx_client, "HTTPX client must be initialised"

        self.api_port: int = self.settings.api_port
        assert self.api_port, "API port must be defined"

        assert self.settings.wallet_name, "Wallet must be defined"
        assert self.settings.hotkey_name, "Hotkey must be defined"

        logger.info(
            f"Attempt to load hotkey keypair with wallet name: {self.settings.wallet_name}"
        )
        self.keypair = chain_utils.load_hotkey_keypair(
            self.settings.wallet_name, self.settings.hotkey_name
        )
        assert self.keypair, "Keypair must be defined"

        self.subtensor_network = self.settings.subtensor.network
        self.subtensor_address = self.settings.subtensor.address
        assert self.subtensor_network, "Subtensor network must be defined"
        assert self.subtensor_address, "Subtensor address must be defined"

        self.substrate = interface.get_substrate(
            subtensor_network=self.subtensor_network,
            subtensor_address=self.subtensor_address,
        )
        assert self.substrate, "Substrate must be defined"

        self.netuid = self.settings.netuid
        self.external_ip = self.settings.external_ip

        assert self.netuid, "Netuid must be defined"

        if self.settings.post_ip:
            assert self.external_ip, "External IP must be defined"
            post_ip_to_chain.post_node_ip_to_chain(
                self.substrate,
                self.keypair,
                self.netuid,
                self.external_ip,
                self.api_port,
                self.keypair.ss58_address,
            )

        self.metagraph = Metagraph(netuid=self.netuid, substrate=self.substrate)
        assert self.metagraph, "Metagraph must be initialised"
        self.metagraph.sync_nodes()

        self.check_registration()
        self.uid = self.metagraph.nodes.get(self.keypair.ss58_address).node_id
        assert self.uid, "UID must be defined"

        self.dht = DHT(self.settings.dht.port)
        assert self.dht, "DHT must be initialised"

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

    def sync_metagraph(self):
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

    def run(self):
        """Background task to sync metagraph"""

        while True:
            try:
                self.sync_metagraph()
                time.sleep(self.settings.neuron.sync_frequency)
            except Exception as e:
                logger.error(f"Error in sync metagraph: {e}")
                time.sleep(self.settings.neuron.sync_frequency // 2)
