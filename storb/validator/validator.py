import asyncio
import copy
import hashlib
import sys
import threading
import time
import typing
from datetime import UTC, datetime
from typing import AsyncGenerator, Literal, override

import aiosqlite
import httpx
import numpy as np
from fastapi import Body, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from fiber.chain import interface
from fiber.chain.weights import set_node_weights
from fiber.encrypted.miner.endpoints.handshake import (
    factory_router as get_subnet_router,
)
from fiber.encrypted.validator import handshake
from fiber.logging_utils import get_logger
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

import storb.validator.db as db
from storb import protocol
from storb.constants import (
    NUM_UIDS_QUERY,
    QUERY_TIMEOUT,
    NeuronType,
)
from storb.dht.piece_dht import ChunkDHTValue, PieceDHTValue
from storb.dht.tracker_dht import TrackerDHTValue
from storb.neuron import Neuron
from storb.util.infohash import generate_infohash
from storb.util.middleware import FileSizeMiddleware, LoggerMiddleware
from storb.util.piece import (
    EncodedChunk,
    Piece,
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
from storb.validator.reward import get_response_rate_scores

logger = get_logger(__name__)


class Validator(Neuron):
    def __init__(self):
        super(Validator, self).__init__(NeuronType.Validator)

        # TODO: have list of connected miners

        self.db_dir = self.settings.validator.db_dir
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

        logger.info("load_state()")
        self.load_state()

        self.loop = asyncio.get_event_loop()

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
            "/metadata/dht",
            self.get_metadata_dht,
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
            "/miners",
            self.get_miners_for_file,
            methods=["POST"],
        )

        self.app.include_router(get_subnet_router())

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

                    self.scores = new_moving_average

                    self.store_latencies = new_store_latencies
                    self.retrieve_latencies = new_ret_latency_scores
                    self.store_latency_scores = new_store_latency_scores
                    self.ret_latency_scores = new_ret_latency_scores
                    self.final_latency_scores = new_final_latency_scores
                    # TODO: use debug for these
                    logger.info(f"(len: {len(self.scores)}) New scores: {self.scores}")
                    logger.info(
                        f"(len: {len(self.store_latencies)}) New store latencies: {self.store_latencies}"
                    )
                    logger.info(
                        f"(len: {len(self.retrieve_latencies)}) New retrieve latencies: {self.retrieve_latencies}"
                    )
                    logger.info(
                        f"(len: {len(self.store_latency_scores)}) New store latency scores: {self.store_latency_scores}"
                    )
                    logger.info(
                        f"(len: {len(self.retrieve_latency_scores)}) New retrieve latency scores: {self.retrieve_latency_scores}"
                    )
                    logger.info(
                        f"(len: {len(self.final_latency_scores)}) New latency scores: {self.final_latency_scores}"
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
            logger.info(f"Scattered rewards: {rewards}")

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
        logger.info(f"weights: {raw_weights}")
        logger.info(f"weights_uids: {weights_uids}")
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

    # TODO: change params?
    async def get_miners_for_file(
        self, request: protocol.GetMiners = Body(...)
    ) -> protocol.GetMiners:
        # TODO: change docstring?
        """Retrieve the list of miners responsible for storing the pieces of a
        file based on its infohash.

        This method looks up all piece IDs associated with the provided infohash
        from a local tracker database, and then retrieves the corresponding miner
        IDs from the DHT. If no pieces are found, an HTTP 404 error is raised.
        If the lookup for any piece's miner fails, an HTTP 500 error is raised.

        Parameters
        ----------
        request: protocol.GetMiners
            A GetMiners instance containing the infohash to look up piece IDs and miners for.

        Returns
        -------
        protocol.GetMiners
            A GetMiners instance populated with the provided infohash, the associated
            piece IDs, and the IDs of the miners that store those pieces.

        Raises
        ------
        HTTPException
            - 404 if no pieces are found for the given infohash.
            - 500 if any error occurs while retrieving miner information from the DHT.
        """

        infohash = request.infohash

        chunk_ids = []
        chunks_metadatas = []
        multi_piece_meta = []

        async with db.get_db_connection(db_dir=self.db_dir) as conn:
            # TODO: erm we shouldn't need to access the array of chunk ids like this?
            # something might be wrong with get_chunks_from_infohash()
            chunk_ids = (await db.get_chunks_from_infohash(conn, infohash))["chunk_ids"]

        if chunk_ids is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="No chunks found for the given infohash",
            )

        for chunk_id in chunk_ids:
            piece_ids = None
            chunks_metadata = await self.dht.get_chunk_entry(chunk_id)
            chunks_metadatas.append(chunks_metadata)
            piece_ids = chunks_metadata.piece_hashes
            pieces_metadata: list[PieceDHTValue] = []
            for piece_id in piece_ids:
                try:
                    piece = await self.dht.get_piece_entry(piece_id)
                    pieces_metadata.append(piece)
                except Exception as e:
                    logger.error(f"Failed to get miner for piece_id {piece_id}: {e}")
                    raise HTTPException(
                        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Failed to get miner for piece_id {piece_id}: {e}",
                    )
            multi_piece_meta.append(pieces_metadata)

        response = protocol.GetMiners(
            infohash=infohash,
            chunk_ids=chunk_ids,
            chunks_metadata=chunks_metadatas,
            pieces_metadata=multi_piece_meta,
        )

        return response

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

        # TODO: challenge miners - based on: https://dl.acm.org/doi/10.1145/1315245.1315318

        # TODO: should we lock the db when scoring?
        # scoring

        # obtain all miner stats from the validator database
        async with db.get_db_connection(self.db_dir) as conn:
            miner_stats = await db.get_all_miner_stats(conn)

        # calculate the score(s) for uids given their stats
        response_rate_uids, response_rate_scores = get_response_rate_scores(
            self, miner_stats
        )

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
            new_scores = np.zeros(new_len)

            len_lat = len(self.store_latencies)
            len_lat_scores = len(self.store_latency_scores)

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

                self.scores = new_scores

        latency_scores = (
            0.5 * self.store_latency_scores + 0.5 * self.retrieve_latency_scores
        )
        self.final_latency_scores = latency_scores / (
            latency_scores.max() if latency_scores.max() != 0 else 1
        )

        # TODO: this should also take the "pdp challenge score" into account
        rewards = 0.2 * self.final_latency_scores + 0.3 * response_rate_scores

        logger.info(f"response rate scores: {response_rate_scores}")
        logger.info(f"moving avg. store latencies: {self.store_latencies}")
        logger.info(f"moving avg. retrieve latencies: {self.retrieve_latencies}")
        logger.info(f"moving avg. store latency scores: {self.store_latency_scores}")
        logger.info(
            f"moving avg. retrieve latency scores: {self.retrieve_latency_scores}"
        )
        logger.info(f"moving avg. latency scores: {self.final_latency_scores}")

        self.update_scores(rewards, response_rate_uids)

        await asyncio.sleep(5)

    async def store_in_dht(
        self,
        validator_id: int,
        infohash: str,
        filename: str,
        filesize: int,
        piece_size: int,
        piece_count: int,
    ):
        """Asynchronously stores tracker entry information in a Distributed Hash Table (DHT).

        Parameters
        ----------
        validator_id : int
            The ID of the validator.
        infohash : str
            The infohash of the file.
        filename : str
            The name of the file.
        filesize : int
            The size of the file in bytes.
        piece_size : int
            The size of each piece in bytes.
        piece_count : int
            The number of pieces the file is divided into.

        Raises
        ------
        HTTPException
            If storing the tracker entry in the DHT fails.
        """

        try:
            await self.dht.store_tracker_entry(
                infohash,
                TrackerDHTValue(
                    validator_id=validator_id,
                    filename=filename,
                    length=filesize,
                    piece_length=piece_size,
                    piece_count=piece_count,
                    parity_count=2,
                ),
            )
        except Exception as e:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to store tracker entry in DHT: {e}",
            )

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
                end_time = time.perf_counter()
                time_elapsed = end_time - start_time
                # TODO: is there a better way to do all this?
                # ig it's totally fine for now
                json_data, file_content = await process_response(received)

                data = None
                if json_data or json_data != {"detail": "Not Found"}:
                    try:
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

    async def process_pieces(
        self,
        pieces: list[Piece],
        hotkeys: list[str],
    ) -> tuple[list[str], list[tuple[str, bytes, int]]]:
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
            """Send batch requests and update miner stats."""

            nonlocal to_query
            tasks = [task for _, task in to_query]
            batch_responses = await asyncio.gather(*tasks)
            for piece_idx, batch in enumerate(batch_responses):
                for uid, recv_payload in batch:
                    if not recv_payload:
                        continue
                    response = recv_payload.data
                    miner_stats[uid]["store_attempts"] += 1
                    logger.debug(f"uid: {uid} response: {response}")
                    if not response:
                        continue

                    # Verify piece hash
                    true_hash = piece_hashes[piece_idx]
                    if response.piece_id == true_hash:
                        latencies[uid] = recv_payload.time_elapsed / len(
                            pieces[piece_idx].data
                        )
                        miner_stats[uid]["store_successes"] += 1
                        miner_stats[uid]["total_successes"] += 1
                        logger.debug(f"Miner {uid} successfully stored {true_hash}")

            to_query = []  # Reset the batch

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

            # Log details
            logger.debug(
                f"piece: {idx} | type: {piece_info.piece_type} | hash: {p_hash} | b64 preview: {piece_info.data[:10]}"
            )

            logger.debug(f"hotkeys to query: {hotkeys}")

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
            to_query.append((idx, task))  # Pair index with the task
            curr_batch_size += 1

            # Send batch requests if batch size is reached
            if curr_batch_size >= self.settings.validator.query.batch_size:
                logger.info(f"Sending {curr_batch_size} batched requests...")
                await handle_batch_requests()
                curr_batch_size = 0

        # Handle any remaining queries
        if to_query:
            logger.info(f"Sending remaining {curr_batch_size} batched requests...")
            await handle_batch_requests()

        logger.debug(f"Processed {len(piece_hashes)} pieces.")

        alpha: float = self.settings.neuron.response_time_alpha
        self.store_latencies = (alpha * latencies) + (1 - alpha) * self.store_latencies
        self.store_latency_scores = 1 - (
            self.retrieve_latencies / self.store_latencies.max()
        )

        # update miner stats table in db
        async with db.get_db_connection(self.db_dir) as conn:
            for miner_uid in uids:
                await db.update_stats(
                    conn,
                    miner_uid=miner_uid,
                    stats=miner_stats[miner_uid],
                )

        return piece_hashes, processed_pieces

    """API Routes"""

    def status(self) -> str:
        return "Hello from Storb!"

    async def get_metadata(self, infohash: str) -> protocol.MetadataResponse:
        """Get file metadata from the local validator database

        Parameters
        ----------
        infohash : str
            The infohash of the file

        Returns
        -------
        protocol.MetadataResponse
        """

        try:
            async with db.get_db_connection(db_dir=self.db_dir) as conn:
                metadata = await db.get_metadata(conn, infohash)

                if metadata is None:
                    # Raise a 404 error if no metadata is found for the given infohash
                    raise HTTPException(status_code=404, detail="Metadata not found")

                return protocol.MetadataResponse(**metadata)

        except aiosqlite.OperationalError as e:
            # Handle database-related errors
            raise HTTPException(status_code=500, detail=f"Database error: {e}")

        except Exception as e:
            # Catch any other unexpected errors
            raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

    async def get_metadata_dht(self, infohash: str) -> protocol.MetadataResponse:
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
            return protocol.MetadataResponse(
                infohash=infohash,
                filename=metadata.filename,
                timestamp=metadata.creation_timestamp,
                piece_length=metadata.piece_length,
                length=metadata.length,
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")

    async def upload_file(self, file: UploadFile = File(...)) -> protocol.StoreResponse:
        """Upload a file"""

        validator_id = self.uid
        request = file

        try:
            print("req headers", request.headers)
            # TODO: currently filename isn't set so fix this
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
            logger.debug(f"hotkeys to query: {hotkeys}")

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

                        print(
                            "len chunk:",
                            len(chunk_buffer),
                            "chunk size:",
                            chunk_size,
                        )

                        encoded_chunk = encode_chunk(chunk_buffer, chunk_idx)
                        sent_piece_hashes, _ = await self.process_pieces(
                            pieces=encoded_chunk.pieces, hotkeys=hotkeys
                        )

                        dht_chunk = ChunkDHTValue(
                            piece_hashes=sent_piece_hashes,
                            chunk_idx=encoded_chunk.chunk_idx,
                            k=encoded_chunk.k,
                            m=encoded_chunk.m,
                            chunk_size=encoded_chunk.chunk_size,
                            padlen=encoded_chunk.padlen,
                            original_chunk_length=encoded_chunk.original_chunk_length,
                        )

                        chunk_hash = hashlib.sha1(
                            dht_chunk.model_dump_json().encode("utf-8")
                        ).hexdigest()

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

            infohash, _ = generate_infohash(
                filename, timestamp, chunk_size, filesize, list(piece_hashes)
            )

            # Store infohash and metadata in the database
            try:
                async with db.get_db_connection(db_dir=self.db_dir) as conn:
                    await db.store_infohash_chunk_ids(conn, infohash, chunk_hashes)
            except aiosqlite.OperationalError as e:
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Database error: {e}",
                )
            except Exception as e:
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Unexpected database error: {e}",
                )

            # Store data object metadata in the DHT
            await self.dht.store_tracker_entry(
                infohash,
                TrackerDHTValue(
                    validator_id=validator_id,
                    filename=filename,
                    length=filesize,
                    chunk_length=chunk_size,
                    chunk_count=len(chunk_hashes),
                    chunk_hashes=chunk_hashes,
                    creation_timestamp=timestamp,
                ),
            )

            logger.info(f"Generated {len(piece_hashes)} unique pieces")
            logger.info(f"Uploaded file with infohash: {infohash}")
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

    # TODO: WIP
    async def get_file(self, infohash: str):
        """Get a file"""

        # Get validator ID from tracker DHT given the infohash
        tracker_dht = await self.dht.get_tracker_entry(infohash)
        validator_to_ask = tracker_dht.validator_id
        validator_hotkey = list(self.metagraph.nodes.keys())[validator_to_ask]

        # Get the uids of the miner(s) that store each respective piece
        # by asking the validator
        # TODO: query miner

        # TODO: ew don't use this language
        synapse = protocol.GetMiners(infohash=infohash)
        payload = Payload(data=synapse)
        _, recv_payload = await self.query_miner(
            miner_hotkey=validator_hotkey,
            endpoint="/miners",
            payload=payload,
        )

        response = recv_payload.data

        # TODO get each group of miners per chunk instead of all at once?
        async with db.get_db_connection(self.db_dir) as conn:
            miner_stats = await db.get_all_miner_stats(conn)

        logger.info(f"Got response from validator {validator_to_ask}")

        piece_ids = []
        pieces = []
        chunks = []
        responses = []

        # TODO: check if the lengths of the chunk ids and chunks_metadata are the same
        for idx, chunk_id in enumerate(response.chunk_ids):
            chunks_metadata: ChunkDHTValue = response.chunks_metadata[idx]
            chunk = EncodedChunk(
                chunk_idx=chunks_metadata.chunk_idx,
                k=chunks_metadata.k,
                m=chunks_metadata.m,
                chunk_size=chunks_metadata.chunk_size,
                padlen=chunks_metadata.padlen,
                original_chunk_length=chunks_metadata.original_chunk_length,
            )
            chunks.append(chunk)

            chunk_pieces_metadata = response.pieces_metadata[idx]

            to_query = []
            for piece_idx, piece_id in enumerate(chunks_metadata.piece_hashes):
                piece_ids.append(piece_id)
                # TODO: many of these can be moved around and placed into their own functions

                # TODO: ew don't use this language
                # get piece(s) from miner(s)
                synapse = protocol.Retrieve(piece_id=piece_id)
                payload = Payload(data=synapse)
                to_query.append(
                    asyncio.create_task(
                        self.query_miner(
                            # TODO: ew
                            miner_hotkey=list(self.metagraph.nodes.keys())[
                                chunk_pieces_metadata[piece_idx].miner_id
                            ],
                            endpoint="/retrieve",
                            payload=payload,
                        )
                    )
                )

            chunk_responses: list[tuple[int, protocol.Retrieve]] = await asyncio.gather(
                *to_query
            )
            responses += chunk_responses

        # TODO: optimize the rest of this
        # check integrity of file if found

        response_piece_ids = []
        piece_ids_match = True

        latencies = np.full(len(self.metagraph.nodes), QUERY_TIMEOUT, dtype=np.float32)

        for idx, uid_and_response in enumerate(responses):
            miner_uid, recv_payload = uid_and_response
            miner_stats[miner_uid]["retrieval_attempts"] += 1

            if not recv_payload:
                continue
            elif not recv_payload.file:
                continue

            piece = recv_payload.file

            piece_id = piece_hash(piece)
            # TODO: do we want to go through all the pieces/uids before returning the error?
            # perhaps we can return an error response, and after we can continue scoring the
            # miners in the background?

            piece_meta = None
            try:
                # TODO: this is probably very inefficient because we got
                # pieces_metadata in the response from the validator
                # cba rn but should probs get around to addressing this later
                piece_meta = await self.dht.get_piece_entry(piece_id)
            except Exception as e:
                logger.error(e)
                piece_meta = None

            if piece_id != piece_ids[idx]:
                piece_ids_match = False
                latencies[miner_uid] = QUERY_TIMEOUT
            else:
                miner_stats[miner_uid]["retrieval_successes"] += 1
                miner_stats[miner_uid]["total_successes"] += 1
                latencies[miner_uid] = recv_payload.time_elapsed / len(piece)

            piece = Piece(
                chunk_idx=piece_meta.chunk_idx,
                piece_idx=piece_meta.piece_idx,
                piece_type=piece_meta.piece_type,
                data=piece,
            )

            pieces.append(piece)
            response_piece_ids.append(piece_id)

        logger.debug(f"tracker_dht: {tracker_dht}")

        # update miner(s) stats in validator db
        async with db.get_db_connection(self.db_dir) as conn:
            for miner_uid, miner_stat in miner_stats.items():
                await db.update_stats(
                    conn,
                    miner_uid=miner_uid,
                    stats=miner_stat,
                )

        if not piece_ids_match:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Piece ids don't not match!",
            )

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

        headers = {
            "Content-Disposition": f"attachment; filename={tracker_dht.filename}"
        }
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
