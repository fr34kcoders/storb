import asyncio
import base64

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fiber.encrypted.miner.endpoints.handshake import (
    factory_router as get_subnet_router,
)
from fiber.logging_utils import get_logger
from fiber.miner.dependencies import blacklist_low_stake, verify_request
from fiber.miner.server import factory_app

from storb import protocol
from storb.dht.piece_dht import PieceDHTValue
from storb.neuron import Neuron
from storb.util.config import MinerConfig
from storb.util.middleware import LoggerMiddleware
from storb.util.piece import piece_hash
from storb.util.store import ObjectStore

logger = get_logger(__name__)


class Miner(Neuron):
    def __init__(self):
        super(Miner, self).__init__()

        load_dotenv()

        self.config = MinerConfig()

        self.check_registration()
        self.uid = self.metagraph.nodes.get(self.keypair.ss58_address).node_id

        self.object_store = ObjectStore(store_dir=self.config.T.store_dir)

        self.app: FastAPI = factory_app()

        self.piece_count: int = 0
        self.request_count: int = 0

        # TODO: set up loggers

    async def start(self):
        self.httpx_client = httpx.AsyncClient()
        self.app_init()
        await self.start_dht()

        asyncio.create_task(self.sync_loop())

        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.config.T.api_port)
        self.server = uvicorn.Server(config)

        try:
            await self.server.serve()
        except Exception as e:
            logger.error(f"Failed to start miner server {e}")
            raise

    async def stop(self):
        if self.server:
            await self.server.stop()

    def app_init(self):
        """Initialise FastAPI routes and middleware"""

        self.app = factory_app(debug=False)

        self.app.add_middleware(LoggerMiddleware)

        self.app.add_api_route(
            "/status",
            self.status,
            methods=["GET"],
        )

        self.app.add_api_route(
            "/piece",
            self.store_piece,
            methods=["POST"],
            response_model=protocol.Store,
            dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        )

        self.app.add_api_route(
            "/piece",
            self.get_piece,
            methods=["GET"],
            response_model=protocol.Retrieve,
            dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        )

        self.app.include_router(get_subnet_router())

    """API routes"""

    async def status(self) -> str:
        return "Hello from the Storb miner!"

    async def store_piece(self, request: protocol.Store) -> protocol.Store:
        """Stores a piece which is received from a validator

        Parameters
        ----------
        request : protocol.Store
            The request object containing the piece to store

        Returns
        -------
        protocol.Store
        """

        logger.debug("Received store request")
        self.request_count += 1

        # TODO: Use raw bytes rather than b64 encoded str
        decoded_piece = base64.b64decode(request.data.encode("utf-8"))
        piece_id = piece_hash(decoded_piece)
        logger.debug(
            f"ptype: {request.piece_type} | piece preview: {decoded_piece[:10]} | hash: {piece_id}"
        )

        await self.object_store.write(piece_id, decoded_piece)
        await self.dht.store_piece_entry(
            piece_id,
            PieceDHTValue(
                miner_id=self.uid,
                chunk_idx=request.chunk_idx,
                piece_idx=request.piece_idx,
                piece_type=request.piece_type,
            ),
        )
        self.piece_count += 1

        response = protocol.Store(
            piece_id=piece_id,
            chunk_idx=request.chunk_idx,
            piece_idx=request.piece_idx,
            piece_type=request.piece_type,
        )

        return response

    async def get_piece(self, request: protocol.Retrieve) -> protocol.Retrieve:
        """Returns a piece from storage

        Parameters
        ----------
        request : protocol.Retrieve
            The request object containing piece metadata

        Returns
        --------
        protocol.Retrieve
        """

        logger.debug("Retrieving piece...")
        logger.debug(f"piece_id to retrieve: {request.piece_id}")

        contents = await self.object_store.read(request.piece_id)
        b64_encoded_piece = base64.b64encode(contents)
        b64_encoded_piece = b64_encoded_piece.decode("utf-8")
        response = protocol.Retrieve(piece_id=request.piece_id, piece=b64_encoded_piece)

        return response
