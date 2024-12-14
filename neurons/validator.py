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
from datetime import UTC, datetime

import aiosqlite
import bittensor as bt
import uvicorn
from fastapi import FastAPI, HTTPException, Request, UploadFile
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

import storage_subnet.validator.db as db

# import base validator class which takes care of most of the boilerplate
from storage_subnet.base.validator import BaseValidatorNeuron
from storage_subnet.constants import (
    EC_DATA_SIZE,
    EC_PARITY_SIZE,
    MAX_UPLOAD_SIZE,
    NUM_UIDS_QUERY,
    LogColor,
)
from storage_subnet.protocol import (
    MetadataResponse,
    MetadataSynapse,
    Store,
    StoreResponse,
)
from storage_subnet.utils.infohash import generate_infohash
from storage_subnet.utils.piece import (
    piece_hash,
    piece_length,
    reconstruct_file,
    split_file,
)
from storage_subnet.utils.uids import get_random_uids
from storage_subnet.validator import forward, query_multiple_miners


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

        # start the axon server
        self.axon.start()

    async def get_metadata(self, synapse: MetadataSynapse) -> MetadataResponse:
        return obtain_metadata(synapse.infohash)

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
async def obtain_metadata(infohash: str) -> MetadataResponse:
    try:
        async with db.get_db_connection() as conn:
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


@app.post("/store/", response_model=StoreResponse)
async def upload_file(file: UploadFile) -> StoreResponse:
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
        piece_hashes = []
        pieces = []

        # Use constants for data/parity pieces
        data_pieces = EC_DATA_SIZE
        parity_pieces = EC_PARITY_SIZE

        timestamp = str(datetime.now(UTC).timestamp())

        # Generate and store all pieces
        # split_file yields (ptype, data, padlen)
        uids = get_random_uids(core_validator, k=NUM_UIDS_QUERY)
        bt.logging.info(f"uids to query: {uids}")

        curr_batch_size = 0
        to_query = []

        for idx, (ptype, data, pad) in enumerate(
            split_file(file, piece_size, data_pieces, parity_pieces)
        ):
            p_hash = piece_hash(data)
            piece_hashes.append(p_hash)
            pieces.append((ptype, data, pad))

            # get ready to upload piece(s) to miner(s)
            # TODO: we base64 encode the data before sending it to the miner(s) for now
            b64_encoded_piece = base64.b64encode(data)
            b64_encoded_piece = b64_encoded_piece.decode("utf-8")

            bt.logging.trace(
                f"piece{idx} | type: {ptype} | piece size: {piece_size} bytes | hash: {p_hash} | b64 preview: {b64_encoded_piece[:10]}"
            )

            # upload piece(s) to miner(s)
            # TODO: this is not optimal - we should probably batch multiple requests
            # and send them at once with asyncio create_task() and gather()
            to_query.append(
                asyncio.create_task(
                    query_multiple_miners(
                        core_validator,
                        synapse=Store(
                            ptype=ptype, piece=b64_encoded_piece, pad_len=pad
                        ),
                        uids=uids,
                    )
                )
            )

            curr_batch_size += 1
            if curr_batch_size >= core_validator.config.query_batch_size:
                bt.logging.info(f"Sending {curr_batch_size} batched requests...")
                batch_responses = await asyncio.gather(*to_query)
                bt.logging.info("Sent batched requests")
                for piece_batch in batch_responses:
                    for uid, response in piece_batch:
                        bt.logging.debug(
                            f"uid: {uid} response: {response.preview_no_piece()}"
                        )

                curr_batch_size = 0
                to_query = []

        bt.logging.trace(f"file checksum: {og_checksum}")

        # TODO: score responses - consider responses that return the piece id to be successful?

        # Generate infohash
        infohash, _ = generate_infohash(
            filename, timestamp, piece_size, filesize, piece_hashes
        )

        # TODO: Remove reconstruction from upload api before shipping
        # Reconstruct the file from the pieces
        reconstructed_data = reconstruct_file(pieces, data_pieces, parity_pieces)
        reconstructed_hash = hashlib.sha256(reconstructed_data).hexdigest()

        bt.logging.trace(f"reconstructed checksum: {reconstructed_hash}")

        # Verify file integrity
        if reconstructed_hash != og_checksum:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST, detail="File integrity check failed"
            )

        # Store infohash and metadata in the database
        try:
            async with db.get_db_connection() as conn:
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
    # check local pieces from infohash

    piece_ids = None
    async with db.get_db_connection() as conn:
        piece_ids = await db.get_pieces_from_infohash(conn, infohash)

    if piece_ids is not None:
        return "found locally"
        # check integrity of file if found
        # reassemble and return file

    # if not in local db query other validators

    # get validator uids
    # vali_uids = await get_query_api_nodes(core_validator.dendrite, core_validator.metagraph)
    active_uids = [
        uid
        for uid in range(core_validator.metagraph.n.item())
        if core_validator.metagraph.axons[uid].is_serving
    ]

    responses = await core_validator.dendrite.forward(
        # Send the query to selected miner axons in the network.
        axons=[core_validator.metagraph.axons[uid] for uid in active_uids],
        # Construct a dummy query. This simply contains a single integer.
        synapse=MetadataSynapse(infohash=infohash),
        # All responses have the deserialize function called on them before returning.
        # You are encouraged to define your own deserialization function.
        # deserialize=True,
        deserialize=False,
    )

    for response in responses:
        print(response)

    # if found, update local tracker db

    # check integrity of file

    # reassemble and return file


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
        log_level="info",
        loop="asyncio",
    )
    server = uvicorn.Server(config)
    await server.serve()


# Combined async main function
async def main() -> None:
    global core_validator  # noqa
    core_validator = Validator()

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
