import asyncio
import base64
from typing import Optional

import httpx
import uvicorn
from fastapi import Body, Depends, FastAPI, File, Form, Request, UploadFile
from fiber.encrypted.miner.endpoints.handshake import (
    factory_router as get_subnet_router,
)
from fiber.logging_utils import get_logger
from fiber.miner.dependencies import blacklist_low_stake, verify_request

from storb import protocol
from storb.constants import NeuronType
from storb.dht.piece_dht import PieceDHTValue
from storb.neuron import Neuron
from storb.util.middleware import LoggerMiddleware
from storb.util.piece import PieceType, piece_hash
from storb.util.query import factory_app
from storb.util.store import ObjectStore

logger = get_logger(__name__)


class Miner(Neuron):
    def __init__(self):
        super(Miner, self).__init__(NeuronType.Miner)

        self.check_registration()
        self.uid = self.metagraph.nodes.get(self.keypair.ss58_address).node_id

        self.object_store = ObjectStore(store_dir=self.settings.store_dir)

        # TODO: set up loggers

    async def start(self):
        self.httpx_client = httpx.AsyncClient()
        self.app_init()
        await self.start_dht()

        asyncio.create_task(self.sync_loop())

        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.settings.api_port)
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

        self.app = factory_app(self.config, debug=False)

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
            # TODO: bring this back
            # dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        )

        self.app.add_api_route(
            "/piece",
            self.get_piece,
            methods=["GET"],
            response_model=protocol.Retrieve,
            # dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        )

        self.app.include_router(get_subnet_router())

    """API routes"""

    async def status(self) -> str:
        return "Hello from the Storb miner!"

    async def store_piece(
        self, json_data: Optional[str] = Form(None), piece: Optional[UploadFile] = None
    ) -> protocol.Store:
        """Stores a piece which is received from a validator.

        Parameters
        ----------
        json_data : Optional[str]
            JSON data containing metadata or additional information about the piece.
        piece : Optional[UploadFile]
            The file object representing the piece to be stored.

        Returns
        -------
        protocol.Store
            The response object containing details of the stored piece.
        """

        logger.debug("Received store request")

        # TODO: Use raw bytes rather than b64 encoded str
        piece_info = protocol.Store.model_validate_json(json_data)
        piece_bytes = await piece.read()
        piece_id = piece_hash(piece_bytes)
        logger.debug(
            f"ptype: {piece_info.piece_type} | piece preview: {piece_bytes[:10]} | hash: {piece_id}"
        )

        await self.object_store.write(piece_id, piece_bytes)
        await self.dht.store_piece_entry(
            piece_id,
            PieceDHTValue(
                miner_id=self.uid,
                chunk_idx=piece_info.chunk_idx,
                piece_idx=piece_info.piece_idx,
                piece_type=piece_info.piece_type,
            ),
        )

        response = protocol.Store(
            piece_id=piece_id,
            chunk_idx=piece_info.chunk_idx,
            piece_idx=piece_info.piece_idx,
            piece_type=piece_info.piece_type,
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
