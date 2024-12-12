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
import time
from datetime import UTC, datetime

import aiosqlite
import bittensor as bt
import uvicorn
from fastapi import FastAPI, HTTPException, Request, UploadFile
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR

import storage_subnet.validator.db as db

# import base validator class which takes care of most of the boilerplate
from storage_subnet.base.validator import BaseValidatorNeuron
from storage_subnet.constants import MAX_UPLOAD_SIZE, LogColor
from storage_subnet.protocol import MetadataResponse, StoreResponse
from storage_subnet.utils.infohash import generate_infohash
from storage_subnet.utils.piece import piece_hash, piece_length, split_file
from storage_subnet.validator import forward


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

        # TODO(developer): Anything specific to your use case you can do here

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
                detail="File must have a valid filename"
            )

        bt.logging.trace(f"content size: {file.size}")

        # Calculate the piece size
        try:
            piece_size = piece_length(file.size)
        except ValueError as e:
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Invalid file size: {e}"
            )

        filename = file.filename
        filesize = file.size

        piece_hashes = []
        timestamp = str(datetime.now(UTC).timestamp())

        # Generate piece hashes
        for idx, piece in enumerate(split_file(file, piece_size)):
            _, data = piece
            p_hash = piece_hash(data)
            piece_hashes.append(p_hash)

            bt.logging.trace(
                f"piece{idx} | piece size: {piece_size} bytes | hash: {p_hash}"
            )

        # Generate infohash
        infohash, _ = generate_infohash(
            filename, timestamp, piece_size, filesize, piece_hashes
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
                detail=f"Database error: {e}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unexpected database error: {e}"
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
            detail=f"Unexpected server error: {e}"
        )


# Async main loop for the validator
async def run_validator(validator: Validator) -> None:
    while True:
        bt.logging.info(f"Validator running... {time.time()}")
        await asyncio.sleep(5)


# Function to run the Uvicorn server
async def run_uvicorn_server() -> None:
    bt.logging.info("Starting API...")
    config = uvicorn.Config(
        app, host="0.0.0.0", port=core_validator.config.api_port, log_level="info"
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
        await asyncio.gather(run_uvicorn_server(), run_validator(core_validator))
    else:
        with core_validator:
            while True:
                bt.logging.debug("Running synthetic vali...")
                await asyncio.sleep(300)


# Entry point
if __name__ == "__main__":
    asyncio.run(main())
