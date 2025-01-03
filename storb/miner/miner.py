import threading
import uuid

from fastapi import Form, UploadFile
from fastapi.responses import StreamingResponse
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
from storb.util.piece import piece_hash
from storb.util.query import factory_app
from storb.util.store import ObjectStore

logger = get_logger(__name__)


class Miner(Neuron):
    def __init__(self):
        super(Miner, self).__init__(NeuronType.Miner)

        self.object_store = ObjectStore(store_dir=self.settings.store_dir)

        # TODO: set up loggers

    async def start(self):
        self.app_init()
        await self.start_dht()

        self.run_in_background_thread()

        try:
            await self.server.serve()
        except Exception as e:
            logger.error(f"Failed to start miner server {e}")
            raise

    def run_in_background_thread(self):
        """
        Starts the miner's operations in a background thread.
        """
        logger.info("Starting background operations in background thread.")
        self.should_exit = False
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
        self.is_running = True
        logger.info("Started")

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
            "/store",
            self.store_piece,
            methods=["POST"],
            response_model=protocol.Store,
            # TODO: bring this back
            # dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        )

        self.app.add_api_route(
            "/retrieve",
            self.get_piece,
            methods=["POST"],
            # response_model=protocol.Retrieve,
            # dependencies=[Depends(blacklist_low_stake), Depends(verify_request)],
        )

        self.app.include_router(get_subnet_router())

    """API routes"""

    async def status(self) -> str:
        return "Hello from the Storb miner!"

    async def store_piece(
        self, json_data: str = Form(None), file: UploadFile = None
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
        piece_bytes = await file.read()
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

    async def get_piece(self, request: protocol.Retrieve):
        """Returns a piece from storage as JSON metadata and a file."""
        logger.debug("Retrieving piece...")
        logger.debug(f"piece_id to retrieve: {request.piece_id}")

        # Fetch the piece from storage
        piece = await self.object_store.read(request.piece_id)

        # Create the JSON metadata response
        metadata = protocol.Retrieve(piece_id=request.piece_id).model_dump_json()

        # Boundary definition
        boundary = str(uuid.uuid4())

        async def iter_response():
            # JSON part
            yield f"--{boundary}\r\n".encode("utf-8")
            yield b"Content-Type: application/json\r\n\r\n"
            yield metadata

            # File part
            if piece:
                yield f"\r\n--{boundary}\r\n".encode("utf-8")
                yield b"Content-Type: application/octet-stream\r\n"
                yield f"Content-Disposition: attachment; filename=piece_{request.piece_id}.bin\r\n\r\n".encode(
                    "utf-8"
                )
                yield piece

            # End boundary
            yield f"\r\n--{boundary}--\r\n".encode("utf-8")

        return StreamingResponse(
            iter_response(), media_type=f"multipart/mixed; boundary={boundary}"
        )
