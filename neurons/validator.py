# The MIT License (MIT)
# Copyright (c) 2023 Yuma Rao
# Copyright (c) 2024 𝓯𝓻𝓮𝓪𝓴𝓬𝓸𝓭𝓮𝓻𝓼

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

import asyncio
import base64
import hashlib
import json
import logging
import logging.config
from datetime import UTC, datetime

import bittensor as bt
import numpy as np
import uvicorn
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

import storage_subnet.validator.db as db

# import base validator class which takes care of most of the boilerplate
from storage_subnet.base.validator import BaseValidatorNeuron
from storage_subnet.constants import (
    DHT_PORT,
    MAX_UPLOAD_SIZE,
    QUERY_TIMEOUT,
    LogColor,
)
from storage_subnet.dht.base_dht import DHT
from storage_subnet.dht.chunk_dht import ChunkDHTValue
from storage_subnet.dht.piece_dht import PieceDHTValue
from storage_subnet.dht.tracker_dht import TrackerDHTValue
from storage_subnet.protocol import (
    GetMiners,
    MetadataResponse,
    MetadataSynapse,
    Retrieve,
    Store,
    StoreResponse,
)
from storage_subnet.utils.infohash import generate_infohash
from storage_subnet.utils.logging import (
    UVICORN_LOGGING_CONFIG,
    setup_event_logger,
    setup_rotating_logger,
)
from storage_subnet.utils.message_signing import (
    ChunkMessage,
    PieceMessage,
    TrackerMessage,
    sign_message,
    verify_message,
)
from storage_subnet.utils.piece import (
    EncodedChunk,
    Piece,
    ProcessedPieceInfo,
    encode_chunk,
    piece_hash,
    piece_length,
    reconstruct_data_stream,
)
from storage_subnet.utils.uids import get_random_uids
from storage_subnet.validator import query_miner, query_multiple_miners
from storage_subnet.validator.reward import get_response_rate_scores


# Define the Validator class
class Validator(BaseValidatorNeuron):
    """
    Your validator neuron class. You should use this class to define your validator's behavior. In particular, you should replace the forward function with your own logic.

    This class inherits from the BaseValidatorNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a validator such as keeping a moving average of the scores of the miners and using them to set weights at the end of each epoch. Additionally, the scores are reset for new hotkeys at the end of each epoch.
    """

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)
        dht_port = (
            self.config.dht.port if hasattr(self.config, "dht.port") else DHT_PORT
        )
        self.dht = DHT(dht_port)
        setup_rotating_logger(
            logger_name="kademlia",
            log_level=logging.DEBUG,
            max_size=5 * 1024 * 1024,  # 5 MiB
        )

        setup_rotating_logger(
            logger_name="rpcudp",
            log_level=logging.DEBUG,
            max_size=5 * 1024 * 1024,  # 5 MiB
        )

        setup_event_logger(
            retention_size=5 * 1024 * 1024  # 5 MiB
        )

        # start the axon server
        self.axon.start()

    async def get_metadata(self, synapse: MetadataSynapse) -> MetadataResponse:
        return obtain_metadata(synapse.infohash)

    async def get_miners_for_file(self, synapse: GetMiners) -> GetMiners:
        """
        Retrieve the list of miners responsible for storing the pieces of a file based on its infohash.

        This method looks up all piece IDs associated with the provided infohash from a local tracker database,
        and then retrieves the corresponding miner IDs from the DHT. If no pieces are found, an HTTP 404 error
        is raised. If the lookup for any piece's miner fails, an HTTP 500 error is raised.

        :param synapse: A GetMiners instance containing the infohash to look up piece IDs and miners for.
        :return: A GetMiners instance populated with the provided infohash, the associated piece IDs,
                 and the IDs of the miners that store those pieces.
        :raises HTTPException:
            - 404 if no pieces are found for the given infohash.
            - 500 if any error occurs while retrieving miner information from the DHT.
        """
        infohash = synapse.infohash

        tracker_data = await self.dht.get_tracker_entry(infohash)
        if tracker_data is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="No tracker entry found for the given infohash",
            )

        chunk_ids = tracker_data.chunk_hashes
        chunks_metadatas = []
        multi_piece_meta = []

        if chunk_ids is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="No chunks found for the given infohash",
            )

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
            piece = None
            for piece_id in piece_ids:
                try:
                    piece = await self.dht.get_piece_entry(piece_id)
                    try:
                        signature = piece.signature
                        # create message object excluding the signature
                        message = PieceMessage(
                            piece_hash=piece_id,
                            miner_id=piece.miner_id,
                            chunk_idx=piece.chunk_idx,
                            piece_idx=piece.piece_idx,
                            piece_type=piece.piece_type,
                        )

                        # verify the signature
                        if not verify_message(
                            self.metagraph, message, signature, piece.miner_id
                        ):
                            raise HTTPException(
                                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                                detail="Signature verification failed!",
                            )
                    except Exception as e:
                        bt.logging.error(e)
                        raise HTTPException(
                            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Signature verification failed: {e}",
                        )
                    pieces_metadata.append(piece)
                except Exception as e:
                    bt.logging.error(
                        f"Failed to get miner for piece_id {piece_id}: {e}"
                    )
                    raise HTTPException(
                        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Failed to get miner for piece_id {piece_id}: {e}",
                    )
            multi_piece_meta.append(pieces_metadata)

        response = GetMiners(
            infohash=infohash,
            chunk_ids=chunk_ids,
            chunks_metadata=chunks_metadatas,
            pieces_metadata=multi_piece_meta,
        )

        return response

    async def forward(self):
        """
        The forward function is called by the validator every time step.

        It is responsible for querying the network and scoring the responses.

        Args:
            self (:obj:`bittensor.neuron.Neuron`): The neuron object which contains all the necessary state for the validator.

        """

        """
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
        async with db.get_db_connection(self.config.db_dir) as conn:
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

        bt.logging.debug(f"response rate scores: {response_rate_scores}")
        bt.logging.debug(f"moving avg. latencies: {self.latencies}")
        bt.logging.debug(f"moving avg. latency scores: {self.latency_scores}")

        # TODO: this should also take the "pdp challenge score" into account
        # TODO: this is a little cooked ngl - will see if it is OK to go without indexing
        rewards = 0.2 * self.latency_scores + 0.3 * response_rate_scores
        self.update_scores(rewards, response_rate_uids)

        await asyncio.sleep(5)

    async def start_dht(self) -> None:
        """
        Start the DHT server.
        """
        try:
            if self.config.dht.bootstrap.ip and self.config.dht.bootstrap.port:
                bt.logging.info(
                    f"Starting DHT server on port {self.config.dht.port} with bootstrap node {self.config.dht.bootstrap.ip}:{self.config.dht.bootstrap.port}"
                )
                bootstrap_ip = (
                    self.config.dht.bootstrap.ip
                    if hasattr(self.config.dht.bootstrap, "dht.bootstrap.ip")
                    else None
                )
                bootstrap_port = (
                    self.config.dht.bootstrap.port
                    if hasattr(self.config.dht.bootstrap, "dht.bootstrap.port")
                    else None
                )
                await self.dht.start(bootstrap_ip, bootstrap_port)
            else:
                bt.logging.info(f"Starting DHT server on port {self.config.dht.port}")
                await self.dht.start()
                bt.logging.info(f"DHT server started on port {self.config.dht.port}")
        except Exception as e:
            bt.logging.error(f"Failed to start DHT server: {e}")
            raise RuntimeError("Failed to start DHT server") from e


core_validator = None


# API setup
app = FastAPI(debug=False)


# logging middleware
@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    # pretty colors for logging

    bt.logging.info(
        f"{LogColor.BOLD}{LogColor.GREEN}Request{LogColor.RESET}: {LogColor.BOLD}{LogColor.BLUE}{request.method}{LogColor.RESET} {request.url}"
    )
    response = await call_next(request)
    return response


@app.middleware("http")
async def check_file_size(request: Request, call_next):
    # Retrieve Content-Length header
    content_length = request.headers.get("Content-Length")
    if content_length:
        if int(content_length) > MAX_UPLOAD_SIZE:
            raise HTTPException(status_code=413, detail="File size exceeds the limit")
    response = await call_next(request)
    return response


@app.get("/status")
async def vali() -> str:
    return "Henlo!"


@app.get("/metadata/", response_model=MetadataResponse)
async def obtain_metadata(infohash: str, request: Request) -> MetadataResponse:
    bt.logging.info(f"Retrieving metadata for infohash: {infohash}")
    try:
        tracker_value = await core_validator.dht.get_tracker_entry(infohash)
        try:
            signature = tracker_value.signature
            # create message object excluding the signature
            message = TrackerMessage(
                infohash=infohash,
                validator_id=tracker_value.validator_id,
                filename=tracker_value.filename,
                length=tracker_value.length,
                chunk_size=tracker_value.chunk_size,
                chunk_count=tracker_value.chunk_count,
                chunk_hashes=tracker_value.chunk_hashes,
                creation_timestamp=tracker_value.creation_timestamp,
            )
            # verify the signature
            if not verify_message(
                core_validator.metagraph, message, signature, tracker_value.validator_id
            ):
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Signature verification failed!",
                )
        except Exception as e:
            bt.logging.error(e)
            tracker_value = None

        return MetadataResponse(
            infohash=infohash,
            filename=tracker_value.filename,
            timestamp=tracker_value.creation_timestamp,
            chunk_size=tracker_value.chunk_size,
            length=tracker_value.length,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


# Upload Helper Functions
async def process_pieces(
    pieces: list[Piece],
    uids: list[str],
    core_validator: Validator,
) -> tuple[list[str], list[tuple[str, bytes, int]]]:
    """
    Process pieces of data by generating their hashes, encoding them, and querying miners.

    Args:
        piece_generator (Iterable[tuple[str, bytes, int]]): An iterable of tuples where each tuple contains:
            - ptype (str): The type of the piece.
            - data (bytes): The data of the piece.
            - pad (int): The padding length of the piece.
        uids (list[str]): A list of unique identifiers for the miners.
        core_validator (Validator): The core validator instance used for querying miners.

    Returns:
        tuple[list[str], list[tuple[str, bytes, int]]]: A tuple containing:
            - A list of piece hashes.
            - A list of processed pieces (ptype, data, pad).
    """
    piece_hashes, processed_pieces, to_query = [], [], []
    curr_batch_size = 0

    async with db.get_db_connection(core_validator.config.db_dir) as conn:
        miner_stats = await db.get_multiple_miner_stats(conn, uids)

    async def handle_batch_requests():
        """Send batch requests and update miner stats."""
        nonlocal to_query
        tasks = [task for _, task in to_query]
        batch_responses = await asyncio.gather(*tasks)
        for piece_idx, batch in enumerate(batch_responses):
            for uid, response in batch:
                miner_stats[uid]["store_attempts"] += 1
                bt.logging.debug(f"uid: {uid} response: {response.preview_no_piece()}")

                # Verify piece hash
                true_hash = piece_hashes[piece_idx]
                if response.piece_id == true_hash:
                    miner_stats[uid]["store_successes"] += 1
                    miner_stats[uid]["total_successes"] += 1
                    bt.logging.trace(f"miner {uid} successfully stored {true_hash}")

        to_query = []  # Reset the batch

    for idx, piece_info in enumerate(pieces):
        p_hash = piece_hash(piece_info.data)
        piece_hashes.append(p_hash)
        processed_pieces.append(
            ProcessedPieceInfo(
                chunk_idx=piece_info.chunk_idx,
                piece_type=piece_info.piece_type,
                piece_idx=piece_info.piece_idx,
                data=piece_info.data,
                piece_id=p_hash,
            )
        )

        # Log details
        # TODO: we base64 encode the data before sending it to the miner(s) for now
        b64_encoded_piece = base64.b64encode(piece_info.data).decode("utf-8")
        bt.logging.trace(
            f"piece{idx} | type: {piece_info.piece_type} | hash: {p_hash} | b64 preview: {piece_info.data[:10]}"
        )

        task = asyncio.create_task(
            query_multiple_miners(
                core_validator,
                synapse=Store(
                    chunk_idx=piece_info.chunk_idx,
                    piece_type=piece_info.piece_type,
                    piece_idx=piece_info.piece_idx,
                    data=b64_encoded_piece,
                ),
                uids=uids,
            )
        )
        to_query.append((idx, task))  # Pair index with the task
        curr_batch_size += 1

        # Send batch requests if batch size is reached
        if curr_batch_size >= core_validator.config.query_batch_size:
            bt.logging.info(f"Sending {curr_batch_size} batched requests...")
            await handle_batch_requests()
            curr_batch_size = 0

    # Handle any remaining queries
    if to_query:
        bt.logging.info(f"Sending remaining {curr_batch_size} batched requests...")
        await handle_batch_requests()

    bt.logging.debug(f"Processed {len(piece_hashes)} pieces.")

    # update miner stats table in db
    async with db.get_db_connection(core_validator.config.db_dir) as conn:
        for miner_uid in uids:
            await db.update_stats(
                conn,
                miner_uid=miner_uid,
                stats=miner_stats[miner_uid],
            )

    return piece_hashes, processed_pieces


@app.post("/store/", response_model=StoreResponse)
async def upload_file(file: UploadFile, req: Request) -> StoreResponse:
    validator_id = core_validator.uid
    try:
        if not file.filename:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail="File must have a valid filename",
            )

        bt.logging.trace(f"content size: {file.size}")

        # Calculate the piece size
        try:
            piece_size = piece_length(file.size)
        except ValueError as e:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail=f"Invalid file size: {e}"
            )

        # file metadata
        filename = file.filename
        filesize = file.size

        # chunk size to read in each iteration
        chunk_size = piece_length(filesize)

        timestamp = str(datetime.now(UTC).timestamp())

        # Generate and store all pieces
        # TODO: better way to do this?
        uids = [
            int(x)
            for x in get_random_uids(
                core_validator, k=core_validator.config.num_uids_query
            )
        ]
        bt.logging.info(f"uids to query: {uids}")

        chunk_hashes = []
        piece_hashes = []

        # we read the file in chunks, and then distribute it in pieces across miners
        for chunk_idx, chunk in enumerate(
            iter(lambda: file.file.read(chunk_size), b"")
        ):
            chunk_info = encode_chunk(chunk, chunk_idx)
            sent_piece_hashes, _ = await process_pieces(
                pieces=chunk_info.pieces,
                uids=uids,
                core_validator=core_validator,
            )

            piece_hashes += sent_piece_hashes

            data = {
                "validator_id": validator_id,
                "piece_hashes": sent_piece_hashes,
                "chunk_idx": chunk_idx,
                "k": chunk_info.k,
                "m": chunk_info.m,
                "chunk_size": chunk_info.chunk_size,
                "padlen": chunk_info.padlen,
                "original_chunk_size": chunk_info.original_chunk_size,
            }
            chunk_hash = hashlib.sha1(json.dumps(data).encode("utf-8")).hexdigest()
            data["chunk_hash"] = chunk_hash

            message = ChunkMessage(
                chunk_hash=chunk_hash,
                validator_id=validator_id,
                piece_hashes=sent_piece_hashes,
                chunk_idx=chunk_info.chunk_idx,
                k=chunk_info.k,
                m=chunk_info.m,
                chunk_size=chunk_info.chunk_size,
                padlen=chunk_info.padlen,
                original_chunk_size=chunk_info.original_chunk_size,
            )

            try:
                signature = sign_message(
                    message=message,
                    wallet=core_validator.wallet,
                )
            except Exception as e:
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to sign chunk: {e}",
                )

            await core_validator.dht.store_chunk_entry(
                chunk_hash,
                ChunkDHTValue(
                    chunk_hash=chunk_hash,
                    validator_id=validator_id,
                    piece_hashes=sent_piece_hashes,
                    chunk_idx=chunk_info.chunk_idx,
                    k=chunk_info.k,
                    m=chunk_info.m,
                    chunk_size=chunk_info.chunk_size,
                    padlen=chunk_info.padlen,
                    original_chunk_size=chunk_info.original_chunk_size,
                    signature=signature,
                ),
            )
            chunk_hashes.append(chunk_hash)

            # TODO: score responses - consider responses that return the piece id to be successful?

        # Generate infohash
        infohash, _ = generate_infohash(
            filename, timestamp, piece_size, filesize, piece_hashes
        )
        # Put piece hashes in a set
        piece_hash_set = set(piece_hashes)
        bt.logging.debug(f"Generated pieces: {len(piece_hash_set)}")
        message = TrackerMessage(
            infohash=infohash,
            validator_id=validator_id,
            filename=filename,
            length=filesize,
            chunk_size=piece_size,
            chunk_count=len(chunk_hashes),
            chunk_hashes=chunk_hashes,
            creation_timestamp=timestamp,
        )

        try:
            signature = sign_message(
                message=message,
                wallet=core_validator.wallet,
            )
        except Exception as e:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to sign metadata: {e}",
            )

        # Store data object metadata in the DHT
        await core_validator.dht.store_tracker_entry(
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

        bt.logging.info(f"Uploaded file with infohash: {infohash}")
        return StoreResponse(infohash=infohash)

    except HTTPException as e:
        # Log HTTP exceptions for debugging
        bt.logging.error(f"HTTP exception: {e.detail}")
        raise

    except Exception as e:
        # Catch any other unexpected errors
        bt.logging.error(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected server error: {e}",
        )


# TODO: WIP
@app.get("/retrieve/")
async def retrieve_file(infohash: str):
    # get validator id from tracker dht given infohash

    try:
        tracker_value = await core_validator.dht.get_tracker_entry(infohash)

        if tracker_value is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="Tracker entry not found.",
            )
        signature = tracker_value.signature
        # create message object excluding the signature
        message = TrackerMessage(
            infohash=infohash,
            validator_id=tracker_value.validator_id,
            filename=tracker_value.filename,
            length=tracker_value.length,
            chunk_size=tracker_value.chunk_size,
            chunk_count=tracker_value.chunk_count,
            chunk_hashes=tracker_value.chunk_hashes,
            creation_timestamp=tracker_value.creation_timestamp,
        )

        # verify the signature
        if not verify_message(
            core_validator.metagraph, message, signature, tracker_value.validator_id
        ):
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Signature verification failed!",
            )
    except Exception as e:
        bt.logging.error(e)
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get tracker entry.",
        )
    validator_to_ask = tracker_value.validator_id

    # get the uids of the miners(s) that stores each respective piece by asking the validator
    synapse = GetMiners(infohash=infohash)
    _, response = await query_miner(
        core_validator,
        synapse=synapse,
        uid=validator_to_ask,
    )

    # TODO get each group of miners per chunk instead of all at once?
    async with db.get_db_connection(core_validator.config.db_dir) as conn:
        miner_stats = await db.get_all_miner_stats(conn)

    bt.logging.info(f"Response from validator {validator_to_ask}: {response}")

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
            original_chunk_size=chunks_metadata.original_chunk_size,
        )
        chunks.append(chunk)

        chunk_pieces_metadata = response.pieces_metadata[idx]

        to_query = []
        for piece_idx, piece_id in enumerate(chunks_metadata.piece_hashes):
            piece_ids.append(piece_id)
            # TODO: many of these can be moved around and placed into their own functions

            # get piece(s) from miner(s)
            synapse = Retrieve(piece_id=piece_id)
            to_query.append(
                asyncio.create_task(
                    query_miner(
                        self=core_validator,
                        synapse=synapse,
                        uid=chunk_pieces_metadata[piece_idx].miner_id,
                        deserialize=True,
                    )
                )
            )

        chunk_responses: list[tuple[int, Retrieve]] = await asyncio.gather(*to_query)
        responses += chunk_responses

    # TODO: optimize the rest of this
    # check integrity of file if found

    response_piece_ids = []
    piece_ids_match = True

    latencies = np.full(core_validator.metagraph.n, QUERY_TIMEOUT, dtype=np.float32)

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
            piece_meta = await core_validator.dht.get_piece_entry(piece_id)
            try:
                signature = piece_meta.signature
                # create message object excluding the signature
                message = PieceMessage(
                    piece_hash=piece_id,
                    miner_id=piece_meta.miner_id,
                    chunk_idx=piece_meta.chunk_idx,
                    piece_idx=piece_meta.piece_idx,
                    piece_type=piece_meta.piece_type,
                )
                # verify the signature
                if not verify_message(
                    core_validator.metagraph, message, signature, piece_meta.miner_id
                ):
                    raise HTTPException(
                        status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Signature verification failed!",
                    )
            except Exception as e:
                bt.logging.error(e)
                piece_meta = None

        except Exception as e:
            bt.logging.error(e)
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

    bt.logging.debug(f"tracker_value: {tracker_value}")

    # update miner(s) stats in validator db
    async with db.get_db_connection(core_validator.config.db_dir) as conn:
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
    alpha: float = core_validator.config.neuron.response_time_alpha
    core_validator.latencies = (alpha * latencies) + (
        1 - alpha
    ) * core_validator.latencies
    core_validator.latency_scores = (
        core_validator.latencies / core_validator.latencies.max()
    )

    # We'll pass them to the streaming generator function:
    def file_generator():
        yield from reconstruct_data_stream(pieces, chunks)

    headers = {"Content-Disposition": f"attachment; filename={tracker_value.filename}"}
    return StreamingResponse(
        file_generator(),
        media_type="application/octet-stream",
        headers=headers,
    )


# Async main loop for the validator
async def run_validator() -> None:
    try:
        core_validator.run_in_background_thread()
    except KeyboardInterrupt:
        bt.logging.info("Keyboard interrupt received, exiting...")


# Function to run the Uvicorn server
async def run_uvicorn_server() -> None:
    bt.logging.info("Starting API...")
    config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=core_validator.config.api_port,
        log_config=logging.config.dictConfig(UVICORN_LOGGING_CONFIG),
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    await server.serve()


# Combined async main function
async def main() -> None:
    global core_validator  # noqa
    core_validator = Validator()
    await core_validator.start_dht()
    if not (core_validator.config.synthetic or core_validator.config.organic):
        bt.logging.error(
            "You did not select a validator type to run! Ensure you select to run either a synthetic or organic validator. Shutting down..."
        )
        return

    bt.logging.info(f"organic: {core_validator.config.organic}")

    if core_validator.config.organic:
        await asyncio.gather(run_uvicorn_server(), run_validator())
    else:
        with core_validator:
            while True:
                bt.logging.debug("Running synthetic vali...")
                await asyncio.sleep(300)


# Entry point
if __name__ == "__main__":
    asyncio.run(main())
