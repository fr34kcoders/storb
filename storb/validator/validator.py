import asyncio
import copy
import hashlib
import queue
import sys
import threading
import time
import typing
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import AsyncGenerator, Literal, override

import httpx
import numpy as np
import uvicorn
from fastapi import File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from fiber.chain import interface
from fiber.chain.weights import set_node_weights
from fiber.encrypted.miner.endpoints.handshake import (
    factory_router as get_subnet_router,
)
from fiber.encrypted.validator import handshake
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

import storb.db as db
from storb import protocol
from storb.challenge import APDPTag, Proof
from storb.constants import (
    NUM_UIDS_QUERY,
    PIECE_CONSUMERS,
    QUERY_TIMEOUT,
    NeuronType,
)
from storb.dht.chunk_dht import ChunkDHTValue
from storb.dht.piece_dht import PieceDHTValue
from storb.dht.tracker_dht import TrackerDHTValue
from storb.neuron import Neuron
from storb.util.infohash import generate_infohash
from storb.util.logging import get_logger
from storb.util.message_signing import (
    ChunkMessage,
    PieceMessage,
    TrackerMessage,
    sign_message,
    verify_message,
)
from storb.util.middleware import FileSizeMiddleware, LoggerMiddleware
from storb.util.piece import (
    EncodedChunk,
    Piece,
    PieceType,
    encode_chunk,
    piece_hash,
    piece_length,
    reconstruct_data_stream,
)
from storb.util.query import (
    Payload,
    factory_app,
    make_non_streamed_get,
    make_non_streamed_post,
    process_response,
)
from storb.util.uids import get_random_hotkeys
from storb.validator.reward import get_challenge_scores, get_response_rate_scores

logger = get_logger(__name__)


@dataclass
class PieceTask:
    piece_idx: int
    piece_hash: str
    data: bytes
    chunk_idx: int
    piece_type: PieceType


@dataclass
class ProcessedPieceResponse:
    piece_hashes: list[str]
    processed_pieces: list[protocol.ProcessedPieceInfo]


class Validator(Neuron):
    def __init__(self):
        super(Validator, self).__init__(NeuronType.Validator)

        # TODO: have list of connected miners

        self.db_dir = self.settings.db_dir
        self.query_timeout = self.settings.validator.query.timeout

        self.symmetric_keys: dict[int, tuple[str, str]] = {}

        self.scores = np.zeros(len(self.metagraph.nodes), dtype=np.float32)
        self.scores_lock = threading.Lock()

        self.store_latencies = np.full(
            len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32
        )
        self.retrieve_latencies = np.full(
            len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32
        )

        self.store_latency_scores = np.zeros(
            len(self.metagraph.nodes), dtype=np.float32
        )
        self.retrieve_latency_scores = np.zeros(
            len(self.metagraph.nodes), dtype=np.float32
        )
        self.final_latency_scores = np.zeros(
            len(self.metagraph.nodes), dtype=np.float32
        )
        self.challenge_scores = np.zeros(len(self.metagraph.nodes), dtype=np.float32)

        # Initialize Challenge dictionary to store challenges sent to miners
        self.challenges: dict[str, protocol.NewChallenge] = {}

        logger.info("load_state()")
        self.load_state()

        self.loop = asyncio.get_event_loop()
        self.piece_queue = queue.Queue()
        self.piece_miners: dict[str, set[int]] = {}
        self.consumer_threads: list[threading.Thread] = []
        self.start_piece_consumers()

    def start_piece_consumers(self, num_consumers: int = PIECE_CONSUMERS):
        """Start threads that continuously consume items from self.piece_queue

        Parameters
        ----------
        num_consumers : int, optional
            Number of threads to start, by default PIECE_CONSUMERS
        """

        for i in range(num_consumers):
            t = threading.Thread(
                target=self.consume_piece_queue, name=f"PieceConsumer-{i+1}"
            )
            t.start()
            self.consumer_threads.append(t)

    async def start(self):
        self.app_init()
        await self.start_dht()

        self.run_in_background_thread()

        try:
            await self.server.serve()
        except Exception as e:
            logger.error(f"Failed to start validator server: {e}")
            raise

    async def stop(self):
        if self.server:
            await self.server.stop()

    def app_init(self):
        """Initialise FastAPI routes and middleware"""

        self.app = factory_app(self.config, debug=False)

        self.app.add_middleware(LoggerMiddleware)
        self.app.add_middleware(FileSizeMiddleware)

        self.app.add_api_route(
            "/status",
            self.status,
            methods=["GET"],
        )

        self.app.add_api_route(
            "/metadata",
            self.get_metadata,
            methods=["GET"],
            response_model=protocol.MetadataResponse,
        )

        self.app.add_api_route(
            "/file",
            self.upload_file,
            methods=["POST"],
            response_model=protocol.StoreResponse,
        )

        self.app.add_api_route(
            "/file",
            self.get_file,
            methods=["GET"],
        )

        self.app.add_api_route(
            "/challenge/verify",
            self.verify_challenge,
            methods=["POST"],
        )

        self.app.include_router(get_subnet_router())

        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.settings.api_port)
        self.server = uvicorn.Server(config)

        assert self.server, "Uvicorn server must be initialised"

    async def perform_handshakes(self):
        logger.info("Performing handshakes with nodes...")
        for node_hotkey, node in self.metagraph.nodes.items():
            try:
                server_addr = f"http://{node.ip}:{node.port}"
                (
                    symmetric_key_str,
                    symmetric_key_uuid,
                ) = await handshake.perform_handshake(
                    keypair=self.keypair,
                    httpx_client=self.httpx_client,
                    server_address=server_addr,
                    miner_hotkey_ss58_address=node_hotkey,
                )
                if symmetric_key_str is None or symmetric_key_uuid is None:
                    logger.error(f"Node {node_hotkey}'s symmetric key or UUID is None")
                    return

                logger.info(f"🤝 Shook hands with Node {node_hotkey}!")

                self.symmetric_keys[node.node_id] = (
                    symmetric_key_str,
                    symmetric_key_uuid,
                )
            except httpx.ConnectTimeout:
                logger.warning("Connection has timed out")
            except Exception as e:
                logger.error(e)

        logger.info("✅ Hands have been shaken!")

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
            previous_metagraph_nodes = copy.deepcopy(self.metagraph.nodes)

            # TODO: are the nodes in order by ascending uid?
            old_hotkeys = list(self.metagraph.nodes.keys())
            self.metagraph.sync_nodes()
            self.metagraph.save_nodes()
            logger.info("Metagraph synced successfully")
            # Check if the metagraph axon info has changed.
            if previous_metagraph_nodes == self.metagraph.nodes:
                return

            logger.info("Metagraph updated, re-syncing hotkeys and moving averages")

            new_hotkeys = list(self.metagraph.nodes.keys())

            with self.scores_lock:
                for uid, hotkey in enumerate(old_hotkeys):
                    if hotkey != new_hotkeys[uid]:
                        self.scores[uid] = 0  # hotkey has been replaced

                # Check to see if the metagraph has changed size.
                # If so, we need to add new hotkeys and moving averages.
                if len(old_hotkeys) < len(new_hotkeys):
                    logger.info(
                        "New uid added to subnet, updating length of scores arrays"
                    )
                    # Update the size of the moving average scores.
                    new_moving_average = np.zeros((len(self.metagraph.nodes)))
                    new_store_latencies = np.full(
                        len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32
                    )
                    new_ret_latencies = np.full(
                        len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32
                    )
                    new_store_latency_scores = np.zeros((len(self.metagraph.nodes)))
                    new_ret_latency_scores = np.zeros((len(self.metagraph.nodes)))
                    new_final_latency_scores = np.zeros((len(self.metagraph.nodes)))
                    new_challenge_scores = np.zeros((len(self.metagraph.nodes)))
                    min_len = min(len(old_hotkeys), len(self.scores))

                    len_store_latencies = min(
                        len(old_hotkeys), len(self.store_latencies)
                    )
                    len_ret_latencies = min(
                        len(old_hotkeys), len(self.retrieve_latencies)
                    )
                    len_store_latency_scores = min(
                        len(old_hotkeys), len(self.store_latency_scores)
                    )
                    len_ret_latency_scores = min(
                        len(old_hotkeys), len(self.retrieve_latency_scores)
                    )
                    len_final_latency_scores = min(
                        len(old_hotkeys), len(self.final_latency_scores)
                    )
                    len_challenge_scores = min(
                        len(old_hotkeys), len(self.challenge_scores)
                    )

                    new_moving_average[:min_len] = self.scores[:min_len]
                    new_store_latencies[:len_store_latencies] = self.store_latencies[
                        :len_store_latencies
                    ]
                    new_ret_latencies[:len_ret_latencies] = self.ret_latencies[
                        :len_ret_latencies
                    ]
                    new_store_latency_scores[:len_store_latency_scores] = (
                        self.store_latency_scores[:len_store_latency_scores]
                    )
                    new_ret_latency_scores[:len_ret_latency_scores] = (
                        self.retrieve_latency_scores[:len_ret_latency_scores]
                    )
                    new_final_latency_scores[:len_final_latency_scores] = (
                        self.final_latency_scores[:len_final_latency_scores]
                    )
                    new_challenge_scores[:len_challenge_scores] = self.challenge_scores[
                        :len_challenge_scores
                    ]

                    self.scores = new_moving_average

                    self.store_latencies = new_store_latencies
                    self.retrieve_latencies = new_ret_latencies
                    self.store_latency_scores = new_store_latency_scores
                    self.ret_latency_scores = new_ret_latency_scores
                    self.final_latency_scores = new_final_latency_scores
                    self.challenge_scores = new_challenge_scores
                    logger.debug(f"(len: {len(self.scores)}) New scores: {self.scores}")
                    logger.debug(
                        f"(len: {len(self.store_latencies)}) New store latencies: {self.store_latencies}"
                    )
                    logger.debug(
                        f"(len: {len(self.retrieve_latencies)}) New retrieve latencies: {self.retrieve_latencies}"
                    )
                    logger.debug(
                        f"(len: {len(self.store_latency_scores)}) New store latency scores: {self.store_latency_scores}"
                    )
                    logger.debug(
                        f"(len: {len(self.retrieve_latency_scores)}) New retrieve latency scores: {self.retrieve_latency_scores}"
                    )
                    logger.debug(
                        f"(len: {len(self.final_latency_scores)}) New latency scores: {self.final_latency_scores}"
                    )
                    logger.debug(
                        f"(len: {len(self.challenge_scores)}) New challenge scores: {self.challenge_scores}"
                    )

        except Exception as e:
            logger.error(f"Failed to sync metagraph: {str(e)}")

    def update_scores(self, rewards: np.ndarray, uids: list[int]):
        """Performs exponential moving average on the scores based on the rewards received from the miners."""

        # Check if rewards contains NaN values.
        if np.isnan(rewards).any():
            logger.warning(f"NaN values detected in rewards: {rewards}")
            # Replace any NaN values in rewards with 0.
            rewards = np.nan_to_num(rewards, nan=0)

        # Ensure rewards is a numpy array.
        rewards = np.asarray(rewards)

        # Check if `uids` is already a numpy array and copy it to avoid the warning.
        if isinstance(uids, np.ndarray):
            uids_array = uids.copy()
        else:
            uids_array = np.array(uids)

        # Handle edge case: If either rewards or uids_array is empty.
        if rewards.size == 0 or uids_array.size == 0:
            logger.info(f"rewards: {rewards}, uids_array: {uids_array}")
            logger.warning(
                "Either rewards or uids_array is empty. No updates will be performed."
            )
            return

        # Check if sizes of rewards and uids_array match.
        if rewards.size != uids_array.size:
            raise ValueError(
                f"Shape mismatch: rewards array of shape {rewards.shape} "
                f"cannot be broadcast to uids array of shape {uids_array.shape}"
            )

        # Compute forward pass rewards, assumes uids are mutually exclusive.
        # shape: [ metagraph.n ]
        with self.scores_lock:
            scattered_rewards: np.ndarray = np.zeros_like(self.scores)
            scattered_rewards[uids_array] = rewards
            # TODO: change some of these logging levels back to "debug"
            logger.debug(f"Scattered rewards: {rewards}")

            # Update scores with rewards produced by this step.
            # shape: [ metagraph.n ]
            alpha: float = self.settings.neuron.moving_average_alpha
            self.scores: np.ndarray = (
                alpha * scattered_rewards + (1 - alpha) * self.scores
            )
            logger.info(f"Updated moving avg scores: {self.scores}")

    def set_weights(self):
        """
        Sets the validator weights to the metagraph hotkeys based on the scores it has received from the miners. The weights determine the trust and incentive level the validator assigns to miner nodes on the network.
        """

        # Check if self.scores contains any NaN values and log a warning if it does.
        with self.scores_lock:
            if np.isnan(self.scores).any():
                logger.warning(
                    "Scores contain NaN values. This may be due to a lack of responses from miners, or a bug in your reward functions."
                )

            # Calculate the average reward for each uid across non-zero values.
            # Replace any NaN values with 0.
            # Compute the norm of the scores
            norm = np.linalg.norm(self.scores, ord=1, axis=0, keepdims=True)

            # Check if the norm is zero or contains NaN values
            if np.any(norm == 0) or np.isnan(norm).any():
                norm = np.ones_like(norm)  # Avoid division by zero or NaN

            # Compute raw_weights safely
            raw_weights = self.scores / norm

        weights_uids = [node.node_id for node in self.metagraph.nodes.values()]

        # TODO: change these to be debug
        logger.debug(f"weights: {raw_weights}")
        logger.debug(f"weights_uids: {weights_uids}")
        # Set the weights
        result = set_node_weights(
            substrate=self.metagraph.substrate,
            keypair=self.keypair,
            node_ids=weights_uids,
            node_weights=raw_weights,
            netuid=self.netuid,
            validator_node_id=self.uid,
            version_key=1,
            max_attempts=2,
        )

        if result:
            logger.info("🏋️ Set weights successfully!")
        else:
            logger.error("Failed to set weights 🙁 - perhaps it is too soon?")

    @override
    def run(self):
        """Background task to sync metagraph"""

        while True:
            try:
                self.sync_metagraph()
                handshake_fut = asyncio.run_coroutine_threadsafe(
                    self.perform_handshakes(), self.loop
                )
                handshake_fut.result()  # Wait for the coroutine to complete
                # TODO: diff logic for organic and synthetic
                # if self.settings.organic:
                forward_fut = asyncio.run_coroutine_threadsafe(
                    self.forward(), self.loop
                )
                forward_fut.result()  # Wait for the coroutine to complete
                # else:
                # self.loop.run_until_complete(self.forward())
                self.set_weights()
                self.save_state()
                time.sleep(self.settings.neuron.sync_frequency)
            except Exception as e:
                logger.error(f"Error in sync loop: {e}")
                time.sleep(self.settings.neuron.sync_frequency // 2)

    def run_in_background_thread(self):
        """
        Starts the validator's operations in a background thread.
        """
        logger.info("Starting backgroud operations in background thread.")
        self.should_exit = False
        self.sync_thread = threading.Thread(target=self.run, daemon=True)
        self.sync_thread.start()
        # let the sync thread do its thing for a bit
        self.is_running = True
        logger.info("Started")

    async def get_infohash(self, infohash: str) -> protocol.GetMinersBase:
        """Retrieve all data associated with the provided infohash from the DHT.

        This method looks up all piece IDs associated with the provided infohash
        from the DHT. If no pieces are found, an HTTP 404 error is raised.
        If the lookup for any piece's miner fails, an HTTP 500 error is raised.

        Parameters
        ----------
        infohash : str
            The infohash of the file to retrieve the miners for.

        Returns
        -------
        protocol.GetMinersBase
            A GetMinersBase instance populated with the provided infohash, the associated
            piece IDs, and the IDs of the miners that store those pieces.

        Raises
        ------
        HTTPException
            - 404 if no pieces are found for the given infohash.
            - 500 if any error occurs while retrieving miner information from the DHT.
        """

        tracker_data = await self.dht.get_tracker_entry(infohash)
        if tracker_data is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="No tracker entry found for the given infohash",
            )

        try:
            signature = tracker_data.signature
            message = TrackerMessage(
                infohash=tracker_data.infohash,
                validator_id=tracker_data.validator_id,
                filename=tracker_data.filename,
                length=tracker_data.length,
                chunk_size=tracker_data.chunk_size,
                chunk_count=tracker_data.chunk_count,
                chunk_hashes=tracker_data.chunk_hashes,
                creation_timestamp=tracker_data.creation_timestamp,
            )
            logger.info(f"hotkeys: {self.metagraph}")
            if not verify_message(
                self.metagraph, message, signature, message.validator_id
            ):
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Signature verification failed for tracker entry",
                )
        except AttributeError:
            logger.error(
                f"Tracker entry {infohash} does not contain a signature attribute"
            )
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Tracker entry {infohash} does not contain a signature attribute",
            )

        chunk_ids = tracker_data.chunk_hashes
        chunks_metadatas = []
        multi_piece_meta = []

        for chunk_id in chunk_ids:
            chunks_metadata = await self.dht.get_chunk_entry(chunk_id)
            if chunks_metadata is None:
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to get chunk for chunk_id {chunk_id}",
                )
            chunks_metadatas.append(chunks_metadata)
            piece_ids = chunks_metadata.piece_hashes
            pieces_metadata: list[PieceDHTValue] = []
            for piece_id in piece_ids:
                try:
                    piece = await self.dht.get_piece_entry(piece_id)
                    logger.info(f"piece: {piece}")
                    try:
                        signature = piece.signature
                        # create message object excluding the signature
                        message = PieceMessage(
                            piece_hash=piece_id,
                            chunk_idx=piece.chunk_idx,
                            piece_idx=piece.piece_idx,
                            piece_type=piece.piece_type,
                        )
                        # verify the signature
                        if not verify_message(
                            self.metagraph, message, signature, piece.validator_id
                        ):
                            logger.error(
                                f"Signature verification failed for piece_id {piece_id}"
                            )
                            raise HTTPException(
                                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                                detail=f"Signature verification failed for piece_id {piece_id}",
                            )
                        pieces_metadata.append(piece)
                    except Exception as e:
                        logger.error(
                            f"Failed to verify signature for piece_id {piece_id}: {e}"
                        )
                        raise HTTPException(
                            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Failed to verify signature",
                        )
                except Exception as e:
                    logger.error(f"Failed to get miner for piece_id {piece_id}: {e}")
                    raise HTTPException(
                        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Failed to get miner",
                    )
            multi_piece_meta.append(pieces_metadata)

        response = protocol.GetMinersBase(
            filename=tracker_data.filename,
            infohash=infohash,
            chunk_ids=chunk_ids,
            chunks_metadata=chunks_metadatas,
            pieces_metadata=multi_piece_meta,
        )

        return response

    async def challenge_miner(self, miner_id: int, piece_id: str, tag: str):
        """Challenge the miners to verify they are storing the pieces

        Parameters
        ----------
        miner_id : int
            The ID of the miner to challenge
        piece_id : str
            The ID of the piece to challenge the miner for
        tag : str
            The tag of the piece to challenge the miner for
        """

        logger.debug(f"Challenging miner {miner_id} for piece {piece_id}")
        # Create the challenge message
        challenge = self.challenge.issue_challenge(tag)
        try:
            signature = sign_message(challenge, self.keypair)
        except Exception as e:
            logger.error(f"Failed to sign challenge: {e}")
            return

        challenge_deadline: datetime = datetime.now(UTC) + timedelta(minutes=15)
        challenge_deadline = challenge_deadline.isoformat()

        challenge_message = protocol.NewChallenge(
            challenge_id=uuid.uuid4().hex,
            piece_id=piece_id,
            validator_id=self.uid,
            miner_id=miner_id,
            challenge_deadline=challenge_deadline,
            public_key=self.challenge.key.rsa.public_key().public_numbers().n,
            public_exponent=self.challenge.key.rsa.public_key().public_numbers().e,
            challenge=challenge,
            signature=signature,
        )

        logger.debug(f"Challenge message: {challenge_message}")
        # Send the challenge to the miner
        miner_hotkey = list(self.metagraph.nodes.keys())[miner_id]
        if miner_hotkey is None:
            logger.error(f"Miner {miner_id} not found in metagraph")
            return

        payload = Payload(
            data=challenge_message,
            file=None,
            time_elapsed=0,
        )
        logger.info(
            f"Sent challenge {challenge_message.challenge_id} to miner {miner_id} for piece {piece_id}"
        )

        async with db.get_db_connection(db_dir=self.db_dir) as conn:
            miner_stats = await db.get_miner_stats(
                conn=conn, miner_uid=challenge_message.miner_id
            )
            miner_stats["challenge_attempts"] += 1
            await db.update_stats(
                conn=conn, miner_uid=challenge_message.miner_id, stats=miner_stats
            )

        logger.debug(f"PRF KEY: {payload.data.challenge.prf_key}")
        _, response = await self.query_miner(
            miner_hotkey, "/challenge", payload, method="POST"
        )

        if response is None:
            logger.error(f"Failed to challenge miner {miner_id}")
            return

        self.challenges[challenge_message.challenge_id] = challenge_message

        logger.debug(f"Received response from miner {miner_id}, response: {response}")
        logger.info(f"Successfully challenged miner {miner_id}")

    # TODO: naming could be better, although this is the Bittensor default
    async def forward(self):
        """The forward function is called by the validator every time step.
        It is responsible for querying the network and scoring the responses.

        Validator forward pass. Consists of:
        - Generating the query
        - Querying the miners
        - Getting the responses
        - Rewarding the miners
        - Updating the scores
        """

        # TODO: should we lock the db when scoring?

        # remove expired challenges
        await self.remove_expired_challenges()

        # scoring
        # obtain all miner stats from the validator database
        async with db.get_db_connection(self.db_dir) as conn:
            miner_stats = await db.get_all_miner_stats(conn)
            try:
                challenge_piece = await db.get_random_piece(conn, self.uid)
            except Exception as e:
                logger.error(f"Failed to get random piece: {e}")
                return

            if isinstance(challenge_piece.miner_id, set):
                random_miner = np.random.choice(list(challenge_piece.miner_id))
                challenge_piece.miner_id = random_miner
            else:
                logger.error(
                    f"Miner ID is not a list: {challenge_piece.miner_id} it is {type(challenge_piece.miner_id)}"
                )
                # skip challenge
                logger.warning(
                    f"Skipping challenge for piece {challenge_piece.piece_hash}, no valid miners found"
                )
                return

        if challenge_piece is not None:
            try:
                await self.challenge_miner(
                    challenge_piece.miner_id,
                    challenge_piece.piece_hash,
                    challenge_piece.tag,
                )
            except Exception as e:
                logger.error(f"Failed to challenge miner: {e}")

        # calculate the score(s) for uids given their stats
        response_rate_uids, response_rate_scores = get_response_rate_scores(
            self, miner_stats
        )

        challenge_uids, self.challenge_scores = get_challenge_scores(self, miner_stats)

        # TODO: error handlin'?
        assert (response_rate_uids == challenge_uids).all()

        if (
            len(self.store_latencies) < len(response_rate_scores)
            or len(self.retrieve_latencies) < len(response_rate_scores)
            or len(self.store_latencies) < len(response_rate_scores)
            or len(self.retrieve_latencies) < len(response_rate_scores)
        ):
            new_len = len(response_rate_scores)
            new_store_latencies = np.full(new_len, QUERY_TIMEOUT, dtype=np.float32)
            new_ret_latencies = np.full(new_len, QUERY_TIMEOUT, dtype=np.float32)
            new_store_latency_scores = np.zeros(new_len)
            new_ret_latency_scores = np.zeros(new_len)
            new_final_latency_scores = np.zeros(new_len)
            new_challenge_scores = np.zeros(new_len)
            new_scores = np.zeros(new_len)

            len_lat = len(self.store_latencies)
            len_lat_scores = len(self.store_latency_scores)
            len_challenge_scores = len(self.challenge_scores)

            with self.scores_lock:
                len_scores = len(self.scores)

                new_store_latencies[:len_lat] = self.store_latencies[:len_lat]
                new_ret_latencies[:len_lat] = self.retrieve_latencies[:len_lat]
                new_store_latency_scores[:len_lat_scores] = self.store_latency_scores[
                    :len_lat_scores
                ]
                new_ret_latency_scores[:len_lat_scores] = self.retrieve_latency_scores[
                    :len_lat_scores
                ]
                new_final_latency_scores[:len_lat_scores] = self.final_latency_scores[
                    :len_lat_scores
                ]
                new_challenge_scores[:len_challenge_scores] = self.challenge_scores[
                    :len_challenge_scores
                ]
                new_scores[:len_scores] = self.scores[:len_scores]

                self.store_latencies = new_store_latencies
                self.retrieve_latencies = new_ret_latencies
                self.store_latency_scores = np.nan_to_num(
                    new_store_latency_scores, nan=0.0
                )
                self.retrieve_latency_scores = np.nan_to_num(
                    new_ret_latency_scores, nan=0.0
                )
                self.final_latency_scores = np.nan_to_num(
                    new_final_latency_scores, nan=0.0
                )
                self.challenge_scores = np.nan_to_num(new_challenge_scores, nan=0.0)
                self.scores = new_scores

        latency_scores = (
            0.5 * self.store_latency_scores + 0.5 * self.retrieve_latency_scores
        )
        self.final_latency_scores = latency_scores / (
            latency_scores.max() if latency_scores.max() != 0 else 1
        )

        rewards = (
            0.2 * self.final_latency_scores
            + 0.3 * response_rate_scores
            + 0.5 * self.challenge_scores
        )

        logger.info(f"response rate scores: {response_rate_scores}")
        logger.info(f"moving avg. store latencies: {self.store_latencies}")
        logger.info(f"moving avg. retrieve latencies: {self.retrieve_latencies}")
        logger.info(f"moving avg. store latency scores: {self.store_latency_scores}")
        logger.info(
            f"moving avg. retrieve latency scores: {self.retrieve_latency_scores}"
        )
        logger.info(f"moving avg. latency scores: {self.final_latency_scores}")
        logger.info(f"challenge rate scores: {self.challenge_scores}")

        self.update_scores(rewards, response_rate_uids)

        await asyncio.sleep(5)

    async def query_miner(
        self,
        miner_hotkey: str,
        endpoint: str,
        payload: Payload,
        method: Literal["GET", "POST"] = "POST",
    ) -> tuple[str, typing.Optional[httpx.Response | Payload]]:
        """Send a query to a miner by making a streamed request"""

        response = None
        node = self.metagraph.nodes.get(miner_hotkey)
        if not node:
            logger.error(
                f"The given miner hotkey {miner_hotkey} does not correspond to any known node"
            )
            return None, response

        server_addr = f"http://{node.ip}:{node.port}"
        symmetric_key = self.symmetric_keys.get(node.node_id)
        if not symmetric_key:
            # logger.warning(f"Entry for node ID {node.node_id} not found")
            return node.node_id, None
        _, symmetric_key_uuid = symmetric_key
        try:
            if method == "GET":
                logger.debug(f"GET {miner_hotkey}")
                received = await make_non_streamed_get(
                    httpx_client=self.httpx_client,
                    server_address=server_addr,
                    validator_ss58_address=self.keypair.ss58_address,
                    symmetric_key_uuid=symmetric_key_uuid,
                    payload=payload,
                    endpoint=endpoint,
                    timeout=QUERY_TIMEOUT,
                )
                # TODO: is there a better way to do all this?
                # ig it's totally fine for now
                response = payload.data.model_validate(received.json())
            if method == "POST":
                logger.debug(f"POST {miner_hotkey}")
                start_time = time.perf_counter()
                try:
                    received = await make_non_streamed_post(
                        httpx_client=self.httpx_client,
                        server_address=server_addr,
                        validator_ss58_address=self.keypair.ss58_address,
                        miner_ss58_address=miner_hotkey,
                        keypair=self.keypair,
                        endpoint=endpoint,
                        payload=payload,
                        timeout=QUERY_TIMEOUT,
                    )
                except Exception as e:
                    logger.error(f"Failed to query miner: {e}")
                    return node.node_id, None
                end_time = time.perf_counter()
                time_elapsed = end_time - start_time
                # TODO: is there a better way to do all this?
                # ig it's totally fine for now
                json_data, file_content = await process_response(received)

                data = None
                if json_data or json_data != {"detail": "Not Found"}:
                    try:
                        logger.debug(f"Payload data: {json_data}")
                        data = payload.data.model_validate(json_data)
                    except Exception:
                        logger.warning(
                            "Payload data validation error, setting it to None"
                        )
                        data = None

                response = Payload(
                    data=data, file=file_content, time_elapsed=time_elapsed
                )

            else:
                # Method not recognised otherwise
                raise ValueError("HTTP method not supported")
        except Exception as e:
            logger.warning(f"could not query miner!: {e}")

        return node.node_id, response

    async def query_multiple_miners(
        self,
        miner_hotkeys: list[str],
        endpoint: str,
        payload: Payload,
        method: Literal["GET", "POST"] = "POST",
    ) -> list[AsyncGenerator[bytes, None] | httpx.Response]:
        """Query multiple miners to store the same payload for redundancy"""

        uid_to_query_task = {
            hk: asyncio.create_task(self.query_miner(hk, endpoint, payload, method))
            for hk in miner_hotkeys
        }

        return await asyncio.gather(*uid_to_query_task.values())

    def process_tag(self, data: bytes) -> str:
        tag = self.challenge.generate_tag(data)
        return tag.model_dump_json()

    # Piece Queue Consumer
    def consume_piece_queue(self):
        """Consume the piece queue and store the pieces in the DHT"""
        while True:
            try:
                piece_task = self.piece_queue.get()
                if piece_task is None:
                    logger.debug("Received None from piece queue, going back to sleep")
                    self.piece_queue.task_done()
                    break
                try:
                    tag = self.process_tag(piece_task.data)
                except Exception as e:
                    logger.error(f"Error processing tag: {e}")
                    self.piece_queue.task_done()
                    continue
                try:
                    message = PieceMessage(
                        piece_hash=piece_task.piece_hash,
                        chunk_idx=piece_task.chunk_idx,
                        piece_idx=piece_task.piece_idx,
                        piece_type=piece_task.piece_type,
                    )

                    signature = sign_message(message, self.keypair)

                except Exception as e:
                    logger.error(f"Error signing message: {e}")
                    self.piece_queue.task_done()
                    continue
                miners = self.piece_miners.get(piece_task.piece_hash)
                logger.debug(f"Piece: {piece_task.piece_hash} | Miners: {miners}")

                self.dht.store_piece_entry(
                    piece_hash=piece_task.piece_hash,
                    value=PieceDHTValue(
                        piece_hash=piece_task.piece_hash,
                        validator_id=self.uid,
                        miner_id=miners,
                        chunk_idx=piece_task.chunk_idx,
                        piece_idx=piece_task.piece_idx,
                        piece_type=piece_task.piece_type,
                        tag=tag,
                        signature=signature,
                    ),
                )
                self.piece_queue.task_done()
            except Exception as e:
                logger.error(f"Error consuming piece task: {e}")
                self.piece_queue.task_done()
                continue

    async def process_pieces(
        self,
        pieces: list[Piece],
        hotkeys: list[str],
    ) -> ProcessedPieceResponse:
        """Process pieces of data by generating their hashes, encoding them,
        and querying miners.

        Parameters
        ----------
        pieces : list[Piece]
            A list of pieces to process
        uids : list[str]
            A list of unique identifiers for the miners.

        Returns
        -------
        tuple[list[str], list[tuple[str, bytes, int]]]
            A tuple containing:
            - A list of piece hashes.
            - A list of processed pieces (ptype, data, pad).
        """
        piece_hashes: list[str] = []
        processed_pieces: list[protocol.ProcessedPieceInfo] = []
        to_query = []
        curr_batch_size = 0

        uids = []
        latencies = np.full(len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32)

        for hotkey in hotkeys:
            uids.append(self.metagraph.nodes[hotkey].node_id)

        async with db.get_db_connection(self.db_dir) as conn:
            miner_stats = await db.get_multiple_miner_stats(conn, uids)

        async def handle_batch_requests():
            """Send batch requests for pieces in `to_query` and update miner stats accordingly."""
            nonlocal to_query

            tasks = [task for _, task in to_query]

            batch_responses = await asyncio.gather(*tasks)

            for i, batch in enumerate(batch_responses):
                real_piece_idx = to_query[i][0]

                for uid, recv_payload in batch:
                    if not recv_payload or not recv_payload.data:
                        continue

                    response = recv_payload.data
                    miner_stats[uid]["store_attempts"] += 1
                    logger.debug(f"uid: {uid} response: {response}")

                    # Verify piece hash
                    true_hash = piece_hashes[real_piece_idx]
                    self.piece_miners.setdefault(true_hash, []).append(uid)
                    self.piece_queue.put(
                        PieceTask(
                            piece_idx=real_piece_idx,
                            piece_hash=true_hash,
                            data=pieces[real_piece_idx].data,
                            chunk_idx=pieces[real_piece_idx].chunk_idx,
                            piece_type=pieces[real_piece_idx].piece_type,
                        )
                    )

                    if response.piece_id == true_hash:
                        latencies[uid] = recv_payload.time_elapsed / len(
                            pieces[real_piece_idx].data
                        )
                        miner_stats[uid]["store_successes"] += 1
                        miner_stats[uid]["total_successes"] += 1

            # Reset for the next batch
            to_query = []

        # Main loop to process each piece
        for idx, piece_info in enumerate(pieces):
            p_hash = piece_hash(piece_info.data)
            piece_hashes.append(p_hash)
            processed_pieces.append(
                protocol.ProcessedPieceInfo(
                    chunk_idx=piece_info.chunk_idx,
                    piece_type=piece_info.piece_type,
                    piece_idx=piece_info.piece_idx,
                    data=piece_info.data,
                    piece_id=p_hash,
                )
            )

            logger.debug(
                f"piece: {idx} | type: {piece_info.piece_type} | hash: {p_hash} "
                f"| b64 preview: {piece_info.data[:10]}"
            )
            logger.debug(f"hotkeys to query: {hotkeys}")

            # Create a storage request task for this piece
            task = asyncio.create_task(
                self.query_multiple_miners(
                    miner_hotkeys=hotkeys,
                    endpoint="/store",
                    payload=Payload(
                        data=protocol.Store(
                            chunk_idx=piece_info.chunk_idx,
                            piece_type=piece_info.piece_type,
                            piece_idx=piece_info.piece_idx,
                        ),
                        file=piece_info.data,
                    ),
                    method="POST",
                )
            )
            # Store (piece_index, task) in to_query
            to_query.append((idx, task))
            curr_batch_size += 1

            # If batch size is reached, send the batch
            if curr_batch_size >= self.settings.validator.query.batch_size:
                logger.info(f"Sending {curr_batch_size} batched requests...")
                await handle_batch_requests()
                curr_batch_size = 0
            logger.debug(f"Queued piece {idx} for storage with miner UIDs: {uids}")

        # Handle any remaining queries in the final batch
        if to_query:
            logger.info(f"Sending remaining {curr_batch_size} batched requests...")
            await handle_batch_requests()

        logger.debug(f"Processed {len(piece_hashes)} pieces.")

        # Update store latency data
        alpha: float = self.settings.neuron.response_time_alpha
        self.store_latencies = alpha * latencies + (1 - alpha) * self.store_latencies
        self.store_latency_scores = 1 - (
            self.store_latencies / self.store_latencies.max()
        )

        # Update miner stats in the database
        async with db.get_db_connection(self.db_dir) as conn:
            for miner_uid in uids:
                await db.update_stats(
                    conn, miner_uid=miner_uid, stats=miner_stats[miner_uid]
                )

        return ProcessedPieceResponse(
            piece_hashes=piece_hashes, processed_pieces=processed_pieces
        )

    async def remove_expired_challenges(self):
        """
        Remove expired challenges from the `self.challenges` dictionary

        Returns:
            None
        """
        now = datetime.now(UTC).isoformat()
        keys_to_remove = []

        for key, challenge in self.challenges.items():
            if challenge.challenge_deadline < now:
                keys_to_remove.append(key)

        if not keys_to_remove:
            return

        for key in keys_to_remove:
            challenge = self.challenges[key]
            del self.challenges[key]

    """API Routes"""

    async def verify_challenge(self, challenge_request: protocol.ProofResponse) -> bool:
        """Verify the challenge proof from the miner

        Parameters
        ----------
        challenge_request : protocol.ProofResponse
            The challenge proof from the miner

        Returns
        -------
        bool
            True if the proof is valid, False otherwise
        """

        logger.debug(f"Verifying challenge proof: {challenge_request.challenge_id}")

        challenge: protocol.NewChallenge = self.challenges.get(
            challenge_request.challenge_id
        )

        if not challenge:
            logger.error(f"Challenge {challenge_request.challenge_id} not found")
            return False

        async with db.get_db_connection(db_dir=self.db_dir) as conn:
            miner_stats = await db.get_miner_stats(conn, challenge.miner_id)

        proof = challenge_request.proof
        try:
            proof = Proof.model_validate(proof)
        except Exception as e:
            logger.error(f"Invalid proof: {e}")
            return False

        if challenge.challenge_deadline < datetime.now(UTC).isoformat():
            logger.error(f"Challenge {challenge_request.challenge_id} has expired")
            return False

        tag = challenge.challenge.tag
        tag = APDPTag.model_validate(tag)
        logger.debug(f"Tag: {tag}")

        logger.debug(
            f"Proof: {proof} \n Challenge: {challenge.challenge} \n tag: {tag} \n n: {challenge.public_key} \n e: {challenge.public_exponent}"
        )

        if not self.challenge.verify_proof(
            proof=proof,
            challenge=challenge.challenge,
            tag=tag,
            n=challenge.public_key,
            e=challenge.public_exponent,
        ):
            logger.error(
                f"Proof verification failed for challenge {challenge.challenge_id}"
            )
            return False

        logger.info(
            f"Proof verification successful for challenge {challenge.challenge_id}"
        )

        async with db.get_db_connection(db_dir=self.db_dir) as conn:
            miner_stats["challenge_successes"] += 1
            await db.update_stats(
                conn=conn, miner_uid=challenge.miner_id, stats=miner_stats
            )

        # remove challenge from memory
        del self.challenges[challenge_request.challenge_id]

        return True

    def status(self) -> str:
        return "Hello from Storb!"

    async def get_metadata(self, infohash: str) -> protocol.MetadataResponse:
        """Get file metadata from the DHT

        Parameters
        ----------
        infohash : str
            The infohash of the file

        Returns
        -------
        protocol.MetadataResponse
        """
        logger.info(f"Retrieving metadata for infohash: {infohash}")
        try:
            metadata = await self.dht.get_tracker_entry(infohash)
            try:
                signature = metadata.signature
                message = TrackerMessage(
                    infohash=metadata.infohash,
                    validator_id=metadata.validator_id,
                    filename=metadata.filename,
                    length=metadata.length,
                    chunk_size=metadata.chunk_size,
                    chunk_count=metadata.chunk_count,
                    chunk_hashes=metadata.chunk_hashes,
                    creation_timestamp=metadata.creation_timestamp,
                )
                if not verify_message(
                    self.metagraph, message, signature, metadata.validator_id
                ):
                    raise HTTPException(
                        status_code=HTTP_400_BAD_REQUEST, detail="Invalid signature"
                    )
            except Exception as e:
                logger.error(f"Error verifying signature: {e}")
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="Metadata does not contain a signature",
                )

            return protocol.MetadataResponse(
                infohash=infohash,
                filename=metadata.filename,
                timestamp=metadata.creation_timestamp,
                chunk_size=metadata.chunk_size,
                length=metadata.length,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

    # TODO: Stream the file upload
    async def upload_file(self, file: UploadFile = File(...)) -> protocol.StoreResponse:
        """Upload a file"""

        validator_id = self.uid
        request = file

        try:
            filename = request.filename
            if not filename:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="File must have a valid filename",
                )

            try:
                filesize = request.size
                logger.debug(f"Content size: {filesize}")
            except ValueError:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail="Invalid file size"
                )

            # Chunk size to read in each iteration
            chunk_size = piece_length(filesize)

            timestamp = str(datetime.now(UTC).timestamp())

            # TODO: Consider miner scores for selection, and not just their availability
            hotkeys = get_random_hotkeys(self, NUM_UIDS_QUERY)

            chunk_hashes = []
            piece_hashes = set()
            done_reading = False

            chunk_idx = 0
            queue = asyncio.Queue()

            async def produce(queue: asyncio.Queue):
                nonlocal done_reading
                nonlocal chunk_size
                try:
                    nonlocal request
                    while True:
                        chunk = await file.read(chunk_size)
                        if not chunk:
                            break
                        await queue.put(chunk)  # Put the chunk in the queue
                    done_reading = True
                except Exception as e:
                    logger.error(f"Error with producer: {e}")

            async def consume(queue: asyncio.Queue):
                nonlocal chunk_idx
                nonlocal piece_hashes
                nonlocal chunk_hashes
                buffer = bytearray()

                while True:
                    read_data = await queue.get()

                    try:
                        buffer.extend(read_data)

                        # TODO: does this cause bug?
                        if len(buffer) < chunk_size and not done_reading:
                            # if len(buffer) < chunk_size:
                            continue

                        # Extract the first READ_SIZE bytes
                        chunk_buffer = copy.copy(buffer[:chunk_size])
                        # Remove them from the buffer
                        del buffer[:chunk_size]

                        logger.debug(
                            "len chunk: %d, chunk size: %d",
                            len(chunk_buffer),
                            chunk_size,
                        )

                        encoded_chunk = encode_chunk(chunk_buffer, chunk_idx)

                        response = await self.process_pieces(
                            pieces=encoded_chunk.pieces, hotkeys=hotkeys
                        )

                        sent_piece_hashes = response.piece_hashes

                        dht_chunk = ChunkDHTValue(
                            validator_id=validator_id,
                            piece_hashes=sent_piece_hashes,
                            chunk_idx=encoded_chunk.chunk_idx,
                            k=encoded_chunk.k,
                            m=encoded_chunk.m,
                            chunk_size=encoded_chunk.chunk_size,
                            padlen=encoded_chunk.padlen,
                            original_chunk_size=encoded_chunk.original_chunk_size,
                        )

                        chunk_hash = hashlib.sha1(
                            dht_chunk.model_dump_json().encode("utf-8")
                        ).hexdigest()

                        dht_chunk.chunk_hash = chunk_hash

                        message = ChunkMessage(
                            chunk_hash=dht_chunk.chunk_hash,
                            validator_id=dht_chunk.validator_id,
                            piece_hashes=dht_chunk.piece_hashes,
                            chunk_idx=dht_chunk.chunk_idx,
                            k=dht_chunk.k,
                            m=dht_chunk.m,
                            chunk_size=dht_chunk.chunk_size,
                            padlen=dht_chunk.padlen,
                            original_chunk_size=dht_chunk.original_chunk_size,
                        )

                        try:
                            signature = sign_message(message, self.keypair)
                            dht_chunk.signature = signature
                        except Exception as e:
                            logger.error(f"Error signing chunk: {e}")
                            raise HTTPException(
                                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                                detail="Error signing chunk",
                            )

                        await self.dht.store_chunk_entry(chunk_hash, dht_chunk)

                        chunk_hashes.append(chunk_hash)
                        piece_hashes.update(sent_piece_hashes)
                        chunk_idx += 1
                    except Exception as e:
                        logger.error(f"Error with consumer: {e}")
                    finally:
                        queue.task_done()

            consumer = asyncio.create_task(consume(queue))
            await produce(queue)

            # Wait until the consumers have processed all the data
            await queue.join()

            # stop consuming
            consumer.cancel()

            await asyncio.gather(consumer, return_exceptions=True)
            logger.critical(f"total piece hashes: {len(piece_hashes)}")
            infohash, _ = generate_infohash(
                filename, timestamp, chunk_size, filesize, list(piece_hashes)
            )

            message = TrackerMessage(
                infohash=infohash,
                validator_id=validator_id,
                filename=filename,
                length=filesize,
                chunk_size=chunk_size,
                chunk_count=len(chunk_hashes),
                chunk_hashes=chunk_hashes,
                creation_timestamp=timestamp,
            )

            try:
                signature = sign_message(message, self.keypair)
            except Exception as e:
                logger.error(f"Error signing tracker message: {e}")
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Error signing tracker message",
                )

            # Store data object metadata in the DHT
            await self.dht.store_tracker_entry(
                infohash,
                TrackerDHTValue(
                    infohash=infohash,
                    validator_id=validator_id,
                    filename=filename,
                    length=filesize,
                    chunk_size=chunk_size,
                    chunk_count=len(chunk_hashes),
                    chunk_hashes=chunk_hashes,
                    creation_timestamp=timestamp,
                    signature=signature,
                ),
            )

            logger.debug(f"Uploaded file with infohash: {infohash}")
            return protocol.StoreResponse(infohash=infohash)

        except HTTPException as e:
            logger.error(f"HTTP exception: {e.detail}")
            raise

        except Exception as e:
            # Catch any other unexpected errors
            logger.error(f"Unexpected error: {e}")
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unexpected server error: {e}",
            )

        except KeyboardInterrupt:
            print("Script terminated by user.")
            sys.exit(0)

    async def get_file(self, infohash: str):
        """Get a file"""
        tracker = await self.get_infohash(infohash)
        if not tracker:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="No miners available for the given file",
            )
        # TODO get each group of miners per chunk instead of all at once?
        async with db.get_db_connection(self.db_dir) as conn:
            miner_stats = await db.get_all_miner_stats(conn)

        pieces = []
        chunks = []
        responses_piece_ids = []
        piece_ids_match = True
        latencies = np.full(len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32)

        # TODO: check if the lengths of the chunk ids and chunks_metadata are the same
        async def get_piece(piece_id, miner_id):
            synapse = protocol.Retrieve(piece_id=piece_id)
            payload = Payload(data=synapse)
            try:
                response = await self.query_miner(
                    miner_hotkey=list(self.metagraph.nodes.keys())[miner_id],
                    endpoint="/retrieve",
                    payload=payload,
                )
                return response
            except Exception as e:
                logger.error(
                    f"Error querying miner {miner_id} for piece {piece_id}: {e}"
                )
                return (list(self.metagraph.nodes.keys())[miner_id], None)

        for idx, _ in enumerate(tracker.chunk_ids):
            chunks_metadata: ChunkDHTValue = tracker.chunks_metadata[idx]
            chunk = EncodedChunk(
                chunk_idx=chunks_metadata.chunk_idx,
                k=chunks_metadata.k,
                m=chunks_metadata.m,
                chunk_size=chunks_metadata.chunk_size,
                padlen=chunks_metadata.padlen,
                original_chunk_size=chunks_metadata.original_chunk_size,
            )
            chunks.append(chunk)

            chunk_pieces_metadata = tracker.pieces_metadata[idx]

            for piece_idx, piece_id in enumerate(chunks_metadata.piece_hashes):
                miner_ids = chunk_pieces_metadata[piece_idx].miner_id
                if not miner_ids:
                    logger.error(
                        f"No miners available for piece {piece_id} in chunk {idx}"
                    )
                    piece_ids_match = False
                    continue
                tasks = [
                    asyncio.create_task(get_piece(piece_id, miner_id))
                    for miner_id in miner_ids
                ]

                for done in asyncio.as_completed(tasks):
                    miner_uid, recv_payload = await done
                    miner_stats[miner_uid]["retrieval_attempts"] += 1

                    if not recv_payload or not recv_payload.file:
                        logger.warning(
                            f"Miner {miner_uid} did not return piece {piece_id}"
                        )
                        continue
                    piece_data = recv_payload.file
                    actual_piece_id = piece_hash(piece_data)

                    if actual_piece_id != piece_id:
                        logger.warning(
                            f"Piece id mismatch: {actual_piece_id} != {piece_id}. Punishing miner {miner_uid}"
                        )
                        latencies[miner_uid] = QUERY_TIMEOUT
                        continue

                    miner_stats[miner_uid]["retrieval_successes"] += 1
                    miner_stats[miner_uid]["total_successes"] += 1
                    latencies[miner_uid] = recv_payload.time_elapsed / len(piece_data)

                    piece = Piece(
                        chunk_idx=chunk.chunk_idx,
                        piece_idx=piece_idx,
                        piece_type=chunk_pieces_metadata[piece_idx].piece_type,
                        data=piece_data,
                    )
                    pieces.append(piece)
                    responses_piece_ids.append(piece_id)

                    for task in tasks:
                        if not task.done():
                            task.cancel()
                    break

            if not piece_ids_match:
                logger.error("Piece ids don't not match!")
                break

        if not piece_ids_match:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Piece integrity verification failed!",
            )

        async with db.get_db_connection(self.db_dir) as conn:
            for miner_uid, miner_stat in miner_stats.items():
                await db.update_stats(conn, miner_uid=miner_uid, stats=miner_stat)

        # we use the ema here so that the latency score changes aren't so sudden
        alpha: float = self.settings.neuron.response_time_alpha
        self.retrieve_latencies = (alpha * latencies) + (
            1 - alpha
        ) * self.retrieve_latencies
        self.retrieve_latency_scores = 1 - (
            self.retrieve_latencies / self.retrieve_latencies.max()
        )

        # We'll pass them to the streaming generator function:
        def file_generator():
            yield from reconstruct_data_stream(pieces, chunks)

        headers = {"Content-Disposition": f"attachment; filename={tracker.filename}"}
        return StreamingResponse(
            file_generator(),
            media_type="application/octet-stream",
            headers=headers,
        )

    def save_state(self):
        """Saves the state of the validator to a file."""
        logger.info("Saving validator state.")

        # Save the state of the validator to file.
        with self.scores_lock:
            np.savez(
                self.settings.full_path / "state.npz",
                scores=self.scores,
                hotkeys=list(self.metagraph.nodes.keys()),
                store_latencies=self.store_latencies,
                retrieve_latencies=self.retrieve_latencies,
                store_latency_scores=self.store_latency_scores,
                retrieve_latency_scores=self.retrieve_latency_scores,
                final_latency_scores=self.final_latency_scores,
            )

    def load_state(self):
        """Loads the state of the validator from a file."""
        logger.info("Loading validator state.")

        # Load the state of the validator from file.
        try:
            state = np.load(self.settings.full_path / "state.npz")
        except FileNotFoundError:
            logger.info("State file was not found, skipping loading state")
            return

        with self.scores_lock:
            self.scores = state["scores"]
        self.hotkeys = state["hotkeys"]
        self.store_latencies = state.get("store_latencies", self.store_latencies)
        self.retrieve_latencies = state.get(
            "retrieve_latencies", self.retrieve_latencies
        )
        self.store_latency_scores = state.get(
            "store_latency_scores", self.store_latency_scores
        )
        self.retrieve_latency_scores = state.get(
            "retrieve_latency_scores", self.retrieve_latency_scores
        )
        self.final_latency_scores = state.get(
            "final_latency_scores", self.final_latency_scores
        )
