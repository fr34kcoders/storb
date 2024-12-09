# The MIT License (MIT)
# Copyright © 2023 Yuma Rao
# Copyright © 2024 𝓯𝓻𝓮𝓪𝓴𝓬𝓸𝓭𝓮𝓻𝓼

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
from io import BytesIO

import bittensor as bt
import uvicorn
from fastapi import FastAPI, HTTPException, Request, UploadFile

# import base validator class which takes care of most of the boilerplate
from storage_subnet.base.validator import BaseValidatorNeuron
from storage_subnet.constants import MAX_UPLOAD_SIZE
from storage_subnet.utils.piece import piece_length
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


@app.post("/store/")
async def upload_file(file: UploadFile):
    buffer = BytesIO()
    bt.logging.trace(f"content size: {file.size}")
    piece_size = piece_length(file.size)
    i = 0
    while content := await file.read(piece_size):
        buffer.write(content)
        bt.logging.trace(
            f"piece{i} | piece size: {piece_size} bytes | preview: {buffer.getvalue()[:10]}"
        )
        # TODO: upload to miner(s) logic
        # clear buffer after uploading :)
        buffer.seek(0)
        buffer.truncate(0)
        i += 1
    return {"message": "File uploaded!", "filename": file.filename}


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
