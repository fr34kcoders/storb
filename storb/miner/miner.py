import asyncio
import datetime
import sys
import threading
import uuid

import uvicorn
from fastapi import Form, UploadFile
from fastapi.responses import StreamingResponse
from fiber.encrypted.miner.endpoints.handshake import (
    factory_router as get_subnet_router,
)
from pydantic import ValidationError

from storb import protocol
from storb.constants import QUERY_TIMEOUT, NeuronType
from storb.neuron import Neuron
from storb.util.logging import get_logger
from storb.util.message_signing import verify_message
from storb.util.middleware import LoggerMiddleware
from storb.util.piece import piece_hash
from storb.util.query import Payload, factory_app, make_non_streamed_post
from storb.util.store import ObjectStore

logger = get_logger(__name__)


class Miner(Neuron):
    def __init__(self):
        super(Miner, self).__init__(NeuronType.Miner)

        self.object_store = ObjectStore(store_dir=self.settings.store_dir)
        self.challenge_queue: asyncio.PriorityQueue[
            tuple[datetime.datetime, protocol.NewChallenge]
        ] = asyncio.PriorityQueue()

    async def start(self):
        self.app_init()
        await self.start_dht()

        self.run_in_background_thread()

        try:
            server_task = asyncio.create_task(self.server.serve())
            consumer_task = asyncio.create_task(self.consume_challenges())
            await asyncio.gather(server_task, consumer_task)
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

        self.app.add_api_route(
            "/challenge",
            self.ack_challenge,
            methods=["POST"],
            response_model=protocol.AckChallenge,
        )

        self.app.include_router(get_subnet_router())

        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.settings.api_port)
        self.server = uvicorn.Server(config)

        assert self.server, "Uvicorn server must be initialised"

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

        logger.info("Received store request")

        piece_info = protocol.Store.model_validate_json(json_data)
        piece_bytes = await file.read()
        piece_id = piece_hash(piece_bytes)
        logger.debug(
            f"ptype: {piece_info.piece_type} | piece preview: {piece_bytes[:10]} | hash: {piece_id}"
        )

        await self.object_store.write(piece_id, piece_bytes)

        response = protocol.Store(
            piece_id=piece_id,
            chunk_idx=piece_info.chunk_idx,
            piece_idx=piece_info.piece_idx,
            piece_type=piece_info.piece_type,
        )

        return response

    async def get_piece(self, request: protocol.Retrieve):
        """Returns a piece from storage as JSON metadata and a file."""

        logger.info("Retrieving piece...")
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

    async def ack_challenge(self, request: protocol.NewChallenge):
        """Acknowledges a challenge from a validator, verifies it, and enqueues it upon success.

        Parameters
        ----------
        request : protocol.NewChallenge
            The challenge request object.

        Returns
        -------
        protocol.AckChallenge
            The response object containing the result of the challenge acknowledgement.
        """

        logger.info(f"Received challenge {request.challenge_id}")

        logger.info(
            f"Verifying challenge {request.challenge_id} wih validator {request.validator_id} and signature {request.signature}"
        )

        if not verify_message(
            self.metagraph,
            request.challenge,
            request.signature,
            request.validator_id,
        ):
            logger.error(
                f"Failed to verify challenge {request.challenge_id} with validator {request.validator_id}"
            )
            return protocol.AckChallenge(
                challenge_id=request.challenge_id, accept=False
            )

        try:
            deadline_dt = datetime.datetime.fromisoformat(request.challenge_deadline)
        except ValueError:
            logger.error(
                "Invalid challenge_deadline format. Must be valid ISO 8601 string."
            )
            return protocol.AckChallenge(
                challenge_id=request.challenge_id, accept=False
            )

        await self.challenge_queue.put((deadline_dt, request))
        logger.info(
            f"Challenge {request.challenge_id} enqueued with deadline {request.challenge_deadline}"
        )

        return protocol.AckChallenge(challenge_id=request.challenge_id, accept=True)

    async def consume_challenges(self):
        """Consumes challenges from the min-heap and processes them."""

        while True:
            try:
                deadline_dt, challenge = await self.challenge_queue.get()
            except Exception:
                logger.exception("Error retrieving challenge from the queue, skipping.")
                continue

            if datetime.datetime.now(datetime.timezone.utc) > deadline_dt:
                logger.info(
                    f"Challenge {challenge.challenge_id} has expired. Skipping."
                )
                self.challenge_queue.task_done()
                continue

            logger.info(f"Processing challenge {challenge.challenge_id}")

            try:
                piece = await self.object_store.read(challenge.piece_id)
                logger.debug(f"Piece retrieved: {piece[:10]}...")
            except FileNotFoundError as e:
                logger.error(
                    f"Failed to retrieve piece {challenge.piece_id} "
                    f"for challenge {challenge.challenge_id}: {e}"
                )
                self.challenge_queue.task_done()
                continue
            except Exception as e:
                logger.exception(
                    f"Unexpected error while reading piece {challenge.piece_id}: {e}"
                )
                self.challenge_queue.task_done()
                continue

            tag = challenge.challenge.tag

            try:
                logger.debug(
                    f"Generating proof for challenge {challenge.challenge_id} \n tag: {tag} \n challenge: {challenge.challenge} \n n: {challenge.public_key}"
                )
                proof = self.challenge.generate_proof(
                    data=piece,
                    tag=tag,
                    n=challenge.public_key,
                    challenge=challenge.challenge,
                )
                logger.debug(f"Proof generated: {proof}")
            except Exception as e:
                logger.error(
                    f"Failed to generate proof for challenge {challenge.challenge_id}: {e}"
                )
                self.challenge_queue.task_done()
                continue

            try:
                payload = Payload(
                    data=protocol.ProofResponse(
                        challenge_id=challenge.challenge_id,
                        piece_id=challenge.piece_id,
                        proof=proof,
                    ),
                    file=None,
                    time_elapsed=0,
                )
            except ValidationError as ve:
                logger.error(f"Validation error when building ProofResponse: {ve}")
                self.challenge_queue.task_done()
                continue
            except Exception as e:
                logger.exception(
                    f"Unexpected error building ProofResponse for challenge {challenge.challenge_id}: {e}"
                )
                self.challenge_queue.task_done()
                continue

            try:
                validator_hotkey = list(self.metagraph.nodes.keys())[
                    challenge.validator_id
                ]
                node = self.metagraph.nodes.get(validator_hotkey)
                if not node:
                    raise KeyError(
                        f"Validator hotkey {validator_hotkey} not found in metagraph."
                    )

                logger.debug(f"Validator hotkey: {validator_hotkey}")
                server_addr = f"http://{node.ip}:{node.port}"
                endpoint = "/challenge/verify"
                logger.debug(f"POST {server_addr}{endpoint}")

                received = await make_non_streamed_post(
                    httpx_client=self.httpx_client,
                    server_address=server_addr,
                    validator_ss58_address=self.keypair.ss58_address,
                    miner_ss58_address=validator_hotkey,
                    keypair=self.keypair,
                    endpoint=endpoint,
                    payload=payload,
                    timeout=QUERY_TIMEOUT,
                )

                if received.status_code == 200:
                    logger.info(
                        f"Proof for challenge {challenge.challenge_id} sent to validator {validator_hotkey}"
                    )
                else:
                    logger.error(
                        f"Failed to send proof for challenge {challenge.challenge_id} "
                        f"to validator {validator_hotkey}, status code {received.status_code}"
                    )
            except KeyError as e:
                logger.error(
                    f"Failed to look up validator for challenge {challenge.challenge_id}: {e}"
                )
            except Exception as e:
                logger.exception(
                    f"Unexpected error sending proof for challenge {challenge.challenge_id}: {e}"
                )
            finally:
                self.challenge_queue.task_done()
