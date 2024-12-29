import asyncio
import base64
import hashlib
from datetime import UTC, datetime
from io import BytesIO
from threading import Lock, Thread
from typing import AsyncGenerator, Literal
from urllib.parse import unquote

import aiosqlite
import httpx
import numpy as np
import uvicorn
import uvicorn.config
from dotenv import load_dotenv
from fastapi import HTTPException, Request
from fiber.chain import chain_utils
from fiber.encrypted.validator import handshake
from fiber.logging_utils import get_logger
from fiber.miner.server import factory_app
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
)
from storb.dht.piece_dht import ChunkDHTValue, PieceDHTValue
from storb.dht.tracker_dht import TrackerDHTValue
from storb.neuron import Neuron
from storb.util.config import ValidatorConfig, add_validator_args
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
from storb.util.query import Payload, make_non_streamed_get, make_streamed_post
from storb.util.uids import get_random_uids
from storb.validator.reward import get_response_rate_scores

logger = get_logger(__name__)


class Validator(Neuron):
    def __init__(self):
        super(Validator, self).__init__()

        load_dotenv()

        self.config = ValidatorConfig()
        add_validator_args(self, self.config._parser)
        self.config.save_config()

        self.keypair = chain_utils.load_hotkey_keypair(
            self.wallet_name, self.hotkey_name
        )

        self.check_registration()
        self.uid = self.metagraph.nodes.get(self.keypair.ss58_address).node_id

        # TODO: have list of connected miners

        self.db_dir = self.config.T.db_dir
        self.query_timeout = self.config.T.query_timeout

        self.uid = self.metagraph.nodes.get(self.keypair.ss58_address).node_id

    async def start(self):
        self.httpx_client = httpx.AsyncClient()
        self.app_init()
        await self.start_dht()

        asyncio.create_task(self.sync_loop())

        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.config.T.validator_api_port,
        )
        self.server = uvicorn.Server(config)

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

        self.app = factory_app(debug=False)

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

    async def get_miners_for_file(
        self, request: protocol.GetMiners
    ) -> protocol.GetMiners:
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

        async with db.get_db_connection(db_dir=self.config.T.db_dir) as conn:
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
        async with db.get_db_connection(self.config.T.db_dir) as conn:
            miner_stats = await db.get_all_miner_stats(conn)

        # calculate the score(s) for uids given their stats
        response_rate_uids, response_rate_scores = get_response_rate_scores(
            self, miner_stats
        )

        if len(self.latency_scores) < len(response_rate_scores) or len(
            self.latencies
        ) < len(response_rate_scores):
            new_len = len(response_rate_scores)
            new_latencies = np.full(new_len, QUERY_TIMEOUT, dtype=np.float32)
            new_latency_scores = np.zeros(new_len)
            new_scores = np.zeros(new_len)

            len_lat = len(self.latencies)
            len_lat_scores = len(self.latency_scores)
            len_scores = len(self.scores)

            new_latencies[:len_lat] = self.latencies[:len_lat]
            new_latency_scores[:len_lat_scores] = self.latency_scores[:len_lat_scores]
            new_scores[:len_scores] = self.scores[:len_scores]

            self.latencies = new_latencies
            self.latency_scores = new_latency_scores
            self.scores = new_scores

        logger.debug(f"response rate scores: {response_rate_scores}")
        logger.debug(f"moving avg. latencies: {self.latencies}")
        logger.debug(f"moving avg. latency scores: {self.latency_scores}")

        # TODO: this should also take the "pdp challenge score" into account
        # TODO: this is a little cooked ngl - will see if it is OK to go without indexing
        rewards = 0.2 * self.latency_scores + 0.3 * response_rate_scores
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
    ) -> AsyncGenerator[bytes, None] | httpx.Response:
        """Send a query to a miner by making a streamed request"""

        node = self.metagraph.nodes.get(miner_hotkey)
        if not node:
            logger.error(
                f"The given miner hotkey {miner_hotkey} does not correspond to any known node"
            )
            return

        server_addr = f"{node.protocol}://{node.ip}:{node.port}"

        symmetric_key_str, symmetric_key_uuid = await handshake.perform_handshake(
            keypair=self.keypair,
            httpx_client=self.httpx_client,
            server_address=server_addr,
            miner_hotkey_ss58_address=miner_hotkey,
        )
        if symmetric_key_str is None or symmetric_key_uuid is None:
            logger.error(f"Miner {miner_hotkey}'s symmetric key or UUID is None")
            return

        if method == "GET":
            return await make_non_streamed_get(
                httpx_client=self.httpx_client,
                server_address=server_addr,
                validator_ss58_address=self.keypair.ss58_address,
                symmetric_key_uuid=symmetric_key_uuid,
                endpoint=endpoint,
                timeout=QUERY_TIMEOUT,
            )

        if method == "POST":
            return await make_streamed_post(
                httpx_client=self.httpx_client,
                server_address=server_addr,
                validator_ss58_address=self.keypair.ss58_address,
                miner_ss58_addres=miner_hotkey,
                keypair=self.keypair,
                endpoint=endpoint,
                payload=payload,
                timeout=QUERY_TIMEOUT,
            )

        # Method not recognised otherwise
        raise ValueError("HTTP method not supported")

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
        uids: list[str],
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

        async with db.get_db_connection(self.config.T.db_dir) as conn:
            miner_stats = await db.get_multiple_miner_stats(conn, uids)

        async def handle_batch_requests():
            """Send batch requests and update miner stats."""

            nonlocal to_query
            tasks = [task for _, task in to_query]
            batch_responses = await asyncio.gather(*tasks)
            for piece_idx, batch in enumerate(batch_responses):
                for uid, response in batch:
                    miner_stats[uid]["store_attempts"] += 1
                    logger.debug(f"uid: {uid} response: {response.preview_no_piece()}")

                    # Verify piece hash
                    true_hash = piece_hashes[piece_idx]
                    if response.piece_id == true_hash:
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

            task = asyncio.create_task(
                self.query_multiple_miners(
                    miner_hotkeys=uids,
                    endpoint="/piece",
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
            if curr_batch_size >= self.config.T.query_batch_size:
                logger.info(f"Sending {curr_batch_size} batched requests...")
                await handle_batch_requests()
                curr_batch_size = 0

        # Handle any remaining queries
        if to_query:
            logger.info(f"Sending remaining {curr_batch_size} batched requests...")
            await handle_batch_requests()

        logger.debug(f"Processed {len(piece_hashes)} pieces.")

        # update miner stats table in db
        async with db.get_db_connection(self.config.T.db_dir) as conn:
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

    async def upload_file(self, request: Request) -> protocol.StoreResponse:
        """Upload a file"""

        validator_id = self.uid

        try:
            print("req headers", request.headers)
            # TODO: currently filename isn't set so fix this
            filename = unquote(request.headers.get("filename") or "filename")
            if not filename:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST,
                    detail="File must have a valid filename",
                )

            try:
                filesize = int(request.headers["Content-Length"])
                logger.debug(f"Content size: {filesize}")
            except ValueError:
                raise HTTPException(
                    status_code=HTTP_400_BAD_REQUEST, detail="Invalid file size"
                )

            # Chunk size to read in each iteration
            chunk_size = piece_length(filesize)

            timestamp = str(datetime.now(UTC).timestamp())

            # TODO: Consider miner scores for selection, and not just their availability
            uids = [int(x) for x in get_random_uids(self, NUM_UIDS_QUERY)]
            logger.debug(f"uids to query: {uids}")

            chunk_hashes = []
            piece_hashes = set()

            chunk_idx = 0
            # shared_buffer = BytesIO()
            # buffer_lock = asyncio.Lock()
            shared_buffer = BytesIO()
            buffer_condition = asyncio.Condition()

            def run_in_thread(coro):
                """Runs an asyncio coroutine in a new thread with its own event loop."""
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(coro)
                finally:
                    loop.close()

            async def producer():
                try:
                    nonlocal request
                    nonlocal shared_buffer
                    async for req_chunk in request.stream():
                        # print("req chunk len:", len(req_chunk), "req chunk:", req_chunk)
                        # with buffer_lock:
                        async with buffer_condition:
                            shared_buffer.write(req_chunk)
                            buffer_condition.notify()  # Notify the consumer
                except Exception as e:
                    logger.error(f"Error with producer: {e}")

            async def consumer():
                nonlocal chunk_idx
                async with buffer_condition:
                    await buffer_condition.wait()  # Wait for the producer to notify
                    len_buffer = shared_buffer.getbuffer().nbytes
                    if len_buffer < chunk_size:
                        await asyncio.sleep(0.1)
                        return

                    shared_buffer.seek(0)  # Move to the beginning of the buffer
                    # TODO: not rlly a buffer ig maybe rename lmao
                    chunk_buffer = shared_buffer.read(chunk_size)
                    shared_buffer.truncate(0)  # Clear the buffer
                    shared_buffer.seek(0)  # Reset the pointer to the beginning

                    chunk_idx += 1 # TODO: do not increment prematurely
                    print(
                        "len chunk:",
                        len(chunk_buffer),
                        "chunk size:",
                        chunk_size,
                        " len shared buffer:",
                        len_buffer,
                    )

                encoded_chunk = encode_chunk(chunk_buffer, chunk_idx)
                sent_piece_hashes, _ = await self.process_pieces(
                    pieces=encoded_chunk.pieces, uids=uids
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

                # TODO: Score responses - consider responses that return the piece id to be successful?

                infohash, _ = generate_infohash(
                    filename, timestamp, chunk_size, filesize, list(piece_hashes)
                )
                logger.debug(f"Generated pieces: {len(piece_hashes)}")

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

                logger.info(f"Uploaded file with infohash: {infohash}")
                return protocol.StoreResponse(infohash=infohash)

            await asyncio.gather(producer(), consumer())
            # consumer_thread = Thread(target=run_in_thread, args=(consumer(), ))
            # producer_thread = Thread(target=run_in_thread, args=(producer(), ))

            # # Start the threads
            # consumer_thread.start()
            # producer_thread.start()

            # # Wait for both threads to complete
            # consumer_thread.join()
            # producer_thread.join()

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

    # TODO: WIP
    async def get_file(self, infohash: str):
        """Get a file"""

        # Get validator ID from tracker DHT given the infohash
        tracker_dht = await self.dht.get_tracker_entry(infohash)
        validator_to_ask = tracker_dht.validator_id

        # Get the uids of the miner(s) that store each respective piece
        # by asking the validator
        # TODO: query miner

        synapse = protocol.GetMiners(infohash=infohash)
        _, response = await self.query_miner(
            miner_hotkey=validator_to_ask,
            endpoint="",  # TODO: make endpoint for miner
            synapse=synapse,
        )

        # TODO get each group of miners per chunk instead of all at once?
        async with db.get_db_connection(self.config.T.db_dir) as conn:
            miner_stats = await db.get_all_miner_stats(conn)

        logger.info(f"Response from validator {validator_to_ask}: {response}")

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

                # get piece(s) from miner(s)
                synapse = protocol.Retrieve(piece_id=piece_id)
                to_query.append(
                    asyncio.create_task(
                        self.query_miner(
                            self=self,
                            synapse=synapse,
                            uid=chunk_pieces_metadata[piece_idx].miner_id,
                            deserialize=True,
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

        latencies = np.full(self.metagraph.n, QUERY_TIMEOUT, dtype=np.float32)

        for idx, uid_and_response in enumerate(responses):
            miner_uid, response = uid_and_response
            miner_stats[miner_uid]["retrieval_attempts"] += 1
            decoded_piece = base64.b64decode(response.piece.encode("utf-8"))
            piece_id = piece_hash(decoded_piece)
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
                latencies[miner_uid] = response.dendrite.process_time

            piece = Piece(
                chunk_idx=piece_meta.chunk_idx,
                piece_idx=piece_meta.piece_idx,
                piece_type=piece_meta.piece_type,
                data=decoded_piece,
            )

            pieces.append(piece)
            response_piece_ids.append(piece_id)

        logger.debug(f"tracker_dht: {tracker_dht}")

        # update miner(s) stats in validator db
        async with db.get_db_connection(self.config.T.db_dir) as conn:
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
        alpha: float = self.config.neuron.response_time_alpha
        self.latencies = (alpha * latencies) + (1 - alpha) * self.latencies
        self.latency_scores = self.latencies / self.latencies.max()

        # We'll pass them to the streaming generator function:
        def file_generator():
            yield from reconstruct_data_stream(pieces, chunks)

        headers = {
            "Content-Disposition": f"attachment; filename={tracker_dht.filename}"
        }
        return protocol.StreamingResponse(
            file_generator(),
            media_type="application/octet-stream",
            headers=headers,
        )
