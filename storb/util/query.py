import email
from email.policy import default
import json
import re
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, AsyncGenerator, Mapping, Optional

import httpx
from fastapi import FastAPI
from fiber import Keypair, utils
from fiber import constants as fcst
from fiber.chain import chain_utils, interface, signatures
from fiber.chain.metagraph import Metagraph
from fiber.logging_utils import get_logger
from fiber.miner.core.configuration import Config, factory_config
from fiber.miner.security import nonce_management
from fiber.validator.generate_nonce import generate_nonce
from pydantic import BaseModel, Field

from storb.config import Config as StorbConfig
from storb.constants import QUERY_TIMEOUT

logger = get_logger(__name__)


@lru_cache
def custom_config(config: StorbConfig) -> Config:
    nonce_manager = nonce_management.NonceManager()

    wallet_name = config.settings.wallet_name
    hotkey_name = config.settings.hotkey_name
    netuid = config.settings.netuid
    subtensor_network = config.settings.subtensor.network
    subtensor_address = config.settings.subtensor.address
    load_old_nodes = True
    # NOTE: This doesn't really have any effect on how the validator operates
    min_stake_threshold = config.settings.min_stake_threshold

    assert netuid is not None, "Must set NETUID env var please!"

    substrate = interface.get_substrate(subtensor_network, subtensor_address)
    metagraph = Metagraph(
        substrate=substrate,
        netuid=netuid,
        load_old_nodes=load_old_nodes,
    )

    keypair = chain_utils.load_hotkey_keypair(wallet_name, hotkey_name)

    return Config(
        nonce_manager=nonce_manager,
        keypair=keypair,
        metagraph=metagraph,
        min_stake_threshold=min_stake_threshold,
        httpx_client=httpx.AsyncClient(),
    )


def factory_app(conf: StorbConfig, debug: bool = False) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = custom_config(conf)
        metagraph = config.metagraph
        sync_thread = None
        if metagraph.substrate is not None:
            sync_thread = threading.Thread(
                target=metagraph.periodically_sync_nodes, daemon=True
            )
            sync_thread.start()

        yield

        logger.info("Shutting down...")

        metagraph.shutdown()
        if metagraph.substrate is not None and sync_thread is not None:
            sync_thread.join()

    app = FastAPI(lifespan=lifespan, debug=debug)

    return app


@dataclass
class Payload:
    """Represents a payload for a request sent via httpx.

    Fields approximate httpx request types, although some types
    have been added or removed.

    Prefer using `data` and `file` fields rather than `content`.
    """

    # content: str | bytes | Iterable[bytes] | AsyncIterable[bytes] | None
    data: Optional[BaseModel | Mapping[str, Any]] = None
    file: Optional[tuple[str, bytes]] = None
    time_elapsed: float = -1.0


# https://github.com/rayonlabs/fiber/blob/production/fiber/validator/client.py


def get_headers_with_nonce(
    payload_str: bytes,
    validator_ss58_address: str,
    miner_ss58_address: str,
    keypair: Keypair,
) -> dict[str, str]:
    """Reimplementation of Fiber's get_headers_with_nonce"""

    nonce = generate_nonce()
    payload_hash = signatures.get_hash(payload_str)
    message = utils.construct_header_signing_message(
        nonce=nonce, miner_hotkey=miner_ss58_address, payload_hash=payload_hash
    )
    signature = signatures.sign_message(keypair, message)
    # To verify this:
    # Get the payload hash, get the signing message, check the hash matches the signature
    return {
        # "Content-Type": "application/json",
        fcst.VALIDATOR_HOTKEY: validator_ss58_address,
        fcst.MINER_HOTKEY: miner_ss58_address,
        fcst.NONCE: nonce,
        fcst.SIGNATURE: signature,
    }


# async def process_response(response: httpx.Response):
#     content_type = response.headers.get("Content-Type", "")

#     if "application/json" in content_type:
#         # If the response is JSON, parse it as JSON
#         json_data = response.json()
#         return json_data, None

#     elif "multipart/mixed" in content_type:
#         # If the response is multipart/mixed, process it
#         boundary = content_type.split("boundary=")[1]
#         parts = response.content.split(f"--{boundary}".encode())

#         json_data = None
#         file_content = None

#         for part in parts:
#             # Check for JSON part
#             if b"Content-Type: application/json" in part:
#                 json_payload = part.split(b"\r\n\r\n")[1]
#                 json_data = json.loads(json_payload.decode("utf-8"))

#             # Check for file part
#             elif b"Content-Type: application/octet-stream" in part:
#                 file_payload = part.split(b"\r\n\r\n")[1]
#                 file_content = file_payload.rstrip(b"--")

#         return json_data, file_content

# else:
#     raise ValueError("Unsupported Content-Type in response.")


# async def process_response(response: httpx.Response):
#     # Check Content-Type header
#     content_type = response.headers.get("Content-Type", "")

#     if "application/json" in content_type:
#         # If the response is JSON, parse and return it
#         json_data = response.json()
#         return json_data, None

#     elif "multipart/mixed" in content_type:
#         # Handle multipart/mixed responses
#         boundary = re.search(r"boundary=(.*)", content_type).group(1)
#         parts = response.content.split(f"--{boundary}".encode())

#         json_data = None
#         file_content = None

#         for part in parts:
#             # Skip empty parts
#             if not part.strip() or part == b"--":
#                 continue

#             # Separate headers and body
#             try:
#                 headers, body = part.split(b"\r\n\r\n", 1)
#             except Exception as e:
#                 # couldn't split this
#                 continue

#             # Check headers to identify part type
#             if b"Content-Type: application/json" in headers:
#                 json_data = json.loads(body.decode("utf-8").strip())

#             elif b"Content-Type: application/octet-stream" in headers:
#                 file_content = body.strip()

#         return json_data, file_content

#     else:
#         raise ValueError("Unsupported Content-Type in response.")


async def process_response(response: httpx.Response):
    # Check Content-Type header
    content_type = response.headers.get("Content-Type", "")

    if "application/json" in content_type:
        # If the response is JSON, parse and return it
        json_data = response.json()
        return json_data, None

    elif "multipart/mixed" in content_type:
        # Handle multipart/mixed responses
        boundary = re.search(r"boundary=(.*)", content_type).group(1)
        parts = response.content.split(f"--{boundary}".encode())

        json_data = None
        file_content = None

        for part in parts:
            # Skip empty or boundary-only parts
            if not part.strip() or part.startswith(b"--"):
                continue

            # Separate headers and body
            try:
                headers, body = part.split(b"\r\n\r\n", 1)
            except ValueError:
                # Skip malformed parts
                continue

            # Check headers to identify part type
            if b"Content-Type: application/json" in headers:
                json_data = json.loads(body.decode("utf-8").strip())

            elif b"Content-Type: application/octet-stream" in headers:
                # Extract the raw file content (without any trailing boundary markers)
                file_content = body.split(b"\r\n--")[0].strip()

        return json_data, file_content


async def make_non_streamed_post(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    validator_ss58_address: str,
    miner_ss58_address: str,
    keypair: Keypair,
    endpoint: str,
    payload: Payload,
    timeout: float = QUERY_TIMEOUT,
) -> httpx.Response:
    # Prepare the headers with a nonce
    content = bytes(str(payload.data or "").encode("utf-8")) + (
        bytes(str(payload.file or "").encode("utf-8")) if payload.file else b""
    )
    headers = get_headers_with_nonce(
        content, validator_ss58_address, miner_ss58_address, keypair
    )

    # Separate JSON and file logic
    if payload.file:
        # If a file is present, send it with the `files` parameter
        files = {
            "file": payload.file,
        }
        # Optionally include JSON data as a separate form field
        if payload.data:
            files["json_data"] = (
                None,
                payload.data.model_dump_json(),
                "application/json",
            )
        response = await httpx_client.post(
            url=server_address + endpoint,
            files=files,
            headers=headers,
            timeout=timeout,
        )
    elif payload.data:
        # If only JSON data is present, use the `json` parameter
        response = await httpx_client.post(
            url=server_address + endpoint,
            json=payload.data.model_dump(),
            headers=headers,
            timeout=timeout,
        )
    else:
        # Handle case where neither JSON nor file is provided
        raise ValueError("Payload must include at least one of `data` or `file`.")

    return response


async def make_streamed_post(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    validator_ss58_address: str,
    miner_ss58_address: str,
    keypair: Keypair,
    endpoint: str,
    # payload: dict[str, Any],
    payload: Payload,
    timeout: float = QUERY_TIMEOUT,
) -> AsyncGenerator[bytes, None]:
    """Make a streamed POST request. Adapted from Fiber's implementation"""

    content = (
        bytes(str(payload.data or "").encode("utf-8"))
        + bytes(str(payload.file or "").encode("utf-8"))
        # + bytes(str(payload.content or "").encode("utf-8"))
    )
    headers = get_headers_with_nonce(
        content, validator_ss58_address, miner_ss58_address, keypair
    )

    match payload:
        case _:
            raise TypeError("Invalid payload type")

    files = {"upload-file": payload.file}

    async with httpx_client.stream(
        method="POST",
        url=server_address + endpoint,
        # content=payload.content,
        data=payload.data,
        files=files,
        headers=headers,
        timeout=timeout,
    ) as response:
        try:
            response.raise_for_status()
            async for line in response.aiter_lines():
                yield line
        except httpx.HTTPStatusError as e:
            await response.aread()
            logger.error(f"HTTP Error {e.response.status_code}: {e.response.text}")
            raise
        except Exception:
            raise


def _get_headers(
    symmetric_key_uuid: str, validator_ss58_address: str
) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        fcst.SYMMETRIC_KEY_UUID: symmetric_key_uuid,
        fcst.VALIDATOR_HOTKEY: validator_ss58_address,
    }


async def make_non_streamed_get(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    validator_ss58_address: str,
    symmetric_key_uuid: str,
    endpoint: str,
    payload: Payload,
    timeout: float = QUERY_TIMEOUT,
) -> httpx.Response:
    headers = _get_headers(symmetric_key_uuid, validator_ss58_address)
    response = await httpx_client.get(
        json=payload.data.model_dump(),
        url=server_address + endpoint,
        headers=headers,
        timeout=timeout,
    )
    return response
