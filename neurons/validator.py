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
import logging
import logging.config
import queue
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Generator, Iterable

import aiosqlite
import bittensor as bt
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
    EC_DATA_SIZE,
    EC_PARITY_SIZE,
    MAX_UPLOAD_SIZE,
    LogColor,
)
from storage_subnet.dht.base_dht import DHT
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
from storage_subnet.utils.piece import (
    piece_hash,
    piece_length,
    split_file,
)
from storage_subnet.utils.uids import get_random_uids
from storage_subnet.validator import forward, query_miner, query_multiple_miners


# Define the Validator class
class Validator(BaseValidatorNeuron):
    """
    Your validator neuron class. You should use this class to define your validator's behavior. In particular, you should replace the forward function with your own logic.

    This class inherits from the BaseValidatorNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a validator such as keeping a moving average of the scores of the miners and using them to set weights at the end of each epoch. Additionally, the scores are reset for new hotkeys at the end of each epoch.
    """

    def __init__(self, config=None):
        super(Validator, self).__init__(config=config)
        bt.logging.info("load_state()")
        self.load_state()
        dht_port = (
            self.config.dht.port if hasattr(self.config, "dht.port") else DHT_PORT
        )
        self.dht = DHT(dht_port)
        setup_rotating_logger(
            logger_name="kademlia",
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
        # Get the pieces from local tracker db
        infohash = synapse.infohash

        piece_ids = None
        async with db.get_db_connection(db_dir=self.config.db_dir) as conn:
            # TODO: erm we shouldn't need to access the array of piece ids like this?
            # something might be wrong with get_pieces_from_infohash()
            piece_ids = (await db.get_pieces_from_infohash(conn, infohash))["piece_ids"]
        if piece_ids is None:
            raise HTTPException(
                status_code=HTTP_404_NOT_FOUND,
                detail="No pieces found for the given infohash",
            )

        miners = []
        for piece_id in piece_ids:
            try:
                piece_metadata = await self.dht.get_piece_entry(piece_id)
                miners.append(piece_metadata.miner_id)
            except Exception as e:
                bt.logging.error(f"Failed to get miner for piece_id {piece_id}: {e}")
                raise HTTPException(
                    status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to get miner for piece_id {piece_id}: {e}",
                )

        response = GetMiners(infohash=infohash, piece_ids=piece_ids, miners=miners)

        return response

    async def forward(self):
        """
        Validator forward pass. Consists of:
        - Generating the query
        - Querying the miners
        - Getting the responses
        - Rewarding the miners
        - Updating the scores
        """
        # TODO(developer): Rewrite this function based on your protocol definition.
        return await forward(self)

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.dht = core_validator.dht
    try:
        yield
    finally:
        await core_validator.dht.stop()


# API setup
app = FastAPI(debug=False, lifespan=lifespan)


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
async def obtain_metadata(infohash: str) -> MetadataResponse:
    try:
        async with db.get_db_connection(db_dir=core_validator.config.db_dir) as conn:
            metadata = await db.get_metadata(conn, infohash)

            if metadata is None:
                # Raise a 404 error if no metadata is found for the given infohash
                raise HTTPException(status_code=404, detail="Metadata not found")

            return MetadataResponse(**metadata)

    except aiosqlite.OperationalError as e:
        # Handle database-related errors
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    except Exception as e:
        # Catch any other unexpected errors
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


@app.get("/metadata/dht/", response_model=MetadataResponse)
async def obtain_metadata_dht(infohash: str, request: Request) -> MetadataResponse:
    dht: DHT = request.app.state.dht
    bt.logging.info(f"Retrieving metadata for infohash: {infohash}")
    try:
        metadata = await dht.get_tracker_entry(infohash)
        return MetadataResponse(
            infohash=infohash,
            filename=metadata.filename,
            timestamp=metadata.creation_timestamp,
            piece_length=metadata.piece_length,
            length=metadata.length,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {e}")


# Upload Helper Functions
async def process_pieces(
    piece_generator: Iterable[tuple[str, bytes, int]],
    piece_size: int,
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
        piece_size (int): The size of each piece in bytes.
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

    for idx, (ptype, data, pad) in enumerate(piece_generator):
        p_hash = piece_hash(data)
        piece_hashes.append(p_hash)
        processed_pieces.append((ptype, data, pad))

        # Log details
        bt.logging.trace(
            f"piece{idx} | type: {ptype} | piece size: {piece_size} bytes | hash: {p_hash} | b64 preview: {data[:10]}"
        )

        # Prepare the query for miners
        b64_encoded_piece = base64.b64encode(data).decode("utf-8")
        task = asyncio.create_task(
            query_multiple_miners(
                core_validator,
                synapse=Store(ptype=ptype, piece=b64_encoded_piece, pad_len=pad),
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


async def store_in_dht(
    dht: DHT,
    validator_id: int,
    infohash: str,
    filename: str,
    filesize: int,
    piece_size: int,
    piece_count: int,
) -> None:
    """
    Asynchronously stores tracker entry information in a Distributed Hash Table (DHT).

    Args:
        dht (DHT): The DHT instance where the tracker entry will be stored.
        validator_id (int): The ID of the validator.
        infohash (str): The infohash of the file.
        filename (str): The name of the file.
        filesize (int): The size of the file in bytes.
        piece_size (int): The size of each piece in bytes.
        piece_count (int): The number of pieces the file is divided into.

    Raises:
        HTTPException: If storing the tracker entry in the DHT fails.
    """
    try:
        await dht.store_tracker_entry(
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


@app.post("/store/", response_model=StoreResponse)
async def upload_file(file: UploadFile, req: Request) -> StoreResponse:
    dht: DHT = req.app.state.dht
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

        # Read the entire file content for checksum
        file_content = await file.read()
        og_checksum = hashlib.sha256(file_content).hexdigest()
        file.file.seek(0)
        filename = file.filename
        filesize = file.size
        bt.logging.trace(f"file checksum: {og_checksum}")

        # Use constants for data/parity pieces
        data_pieces = EC_DATA_SIZE
        parity_pieces = EC_PARITY_SIZE

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

        pieces_generator = split_file(file, piece_size, data_pieces, parity_pieces)

        # process pieces, send them to miners, and update their stats
        piece_hashes, _ = await process_pieces(
            piece_size=piece_size,
            piece_generator=pieces_generator,
            uids=uids,
            core_validator=core_validator,
        )

        # Generate infohash
        infohash, _ = generate_infohash(
            filename, timestamp, piece_size, filesize, piece_hashes
        )
        # Put piece hashes in a set
        piece_hash_set = set(piece_hashes)
        bt.logging.debug(f"Generated pieces: {len(piece_hash_set)}")
        # Store infohash and metadata in the database
        try:
            async with db.get_db_connection(
                db_dir=core_validator.config.db_dir
            ) as conn:
                await db.store_infohash_piece_ids(conn, infohash, piece_hashes)
                await db.store_metadata(
                    conn, infohash, filename, timestamp, piece_size, filesize
                )
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

        # Store metadata in the DHT
        await store_in_dht(
            dht=dht,
            validator_id=validator_id,
            infohash=infohash,
            filename=filename,
            filesize=filesize,
            piece_size=piece_size,
            piece_count=len(piece_hashes),
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
    tracker_dht = await core_validator.dht.get_tracker_entry(infohash)
    validator_to_ask = tracker_dht.validator_id

    # get the uids of the miners(s) that stores each respective piece by asking the validator
    synapse = GetMiners(infohash=infohash)
    _, response = await query_miner(
        core_validator,
        synapse=synapse,
        uid=validator_to_ask,
    )

    bt.logging.info(f"Response from validator {validator_to_ask}: {response}")

    piece_ids = response.piece_ids
    miners = response.miners

    async with db.get_db_connection(core_validator.config.db_dir) as conn:
        miner_stats = await db.get_multiple_miner_stats(conn, miners)

    # TODO: check if piece_ids and miners lengths are the same
    # TODO: many of these can be moved around and placed into their own functions

    # get piece(s) from miner(s)
    to_query = []
    for idx, miner_uid in enumerate(miners):
        piece_id = piece_ids[idx]
        synapse = Retrieve(piece_id=piece_id)
        to_query.append(
            asyncio.create_task(
                query_miner(
                    self=core_validator,
                    synapse=synapse,
                    uid=miner_uid,
                    deserialize=True,
                )
            )
        )

    responses: list[tuple[int, Retrieve]] = await asyncio.gather(*to_query)

    # TODO: optimize the rest of this
    # check integrity of file if found

    response_piece_ids = []
    piece_ids_match = True

    for idx, uid_and_response in enumerate(responses):
        miner_uid, response = uid_and_response
        miner_stats[miner_uid]["retrieval_attempts"] += 1
        decoded_piece = base64.b64decode(response.piece.encode("utf-8"))
        piece_id = piece_hash(decoded_piece)
        # TODO: do we want to go through all the pieces/uids before returning the error?
        # perhaps we can return an error response, and after we can continue scoring the
        # miners in the background?
        if piece_id != piece_ids[idx]:
            piece_ids_match = False
        else:
            miner_stats[miner_uid]["retrieval_successes"] += 1
            miner_stats[miner_uid]["total_successes"] += 1
        response_piece_ids.append(piece_id)

    bt.logging.debug(f"tracker_dht: {tracker_dht}")

    # update miner(s) stats in validator db
    async with db.get_db_connection(core_validator.config.db_dir) as conn:
        for miner_uid in miners:
            await db.update_stats(
                conn,
                miner_uid=miner_uid,
                stats=miner_stats[miner_uid],
            )

    if not piece_ids_match:
        raise HTTPException(
            status_code=HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Piece ids don't not match!",
        )

    # stream the pieces as a file
    buffer = queue.Queue()

    # Function to add items to the buffer
    async def fill_buffer():
        for _, response in responses:
            piece = base64.b64decode(response.piece.encode("utf-8"))
            buffer.put(piece)  # Add to buffer
        buffer.put(None)  # Signal end of stream by adding None

    # Function to generate the stream
    def stream_from_buffer() -> Generator[bytes, None, None]:
        while True:
            chunk = buffer.get()  # Block until data is available
            if chunk is None:  # If None is received, end of the stream
                break
            yield chunk  # Yield the data chunk to the client

    asyncio.create_task(fill_buffer())

    headers = {"Content-Disposition": f"attachment; filename={tracker_dht.filename}"}
    return StreamingResponse(
        stream_from_buffer(), media_type="application/octet-stream", headers=headers
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
