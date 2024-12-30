from dataclasses import dataclass
from typing import Any, AsyncGenerator, AsyncIterable, Iterable, Mapping

import httpx
from fiber import Keypair, utils
from fiber import constants as fcst
from fiber.chain import signatures
from fiber.logging_utils import get_logger
from fiber.validator.generate_nonce import generate_nonce
from pydantic import BaseModel

from storb.constants import QUERY_TIMEOUT

logger = get_logger(__name__)


@dataclass
class Payload:
    """Represents a payload for a request sent via httpx.

    Fields approximate httpx request types, although some types
    have been added or removed.

    Prefer using `data` and `file` fields rather than `content`.
    """

    # content: str | bytes | Iterable[bytes] | AsyncIterable[bytes] | None
    data: BaseModel | Mapping[str, Any] | None
    file: tuple[str, bytes] | None


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


async def make_non_streamed_post(
    httpx_client: httpx.AsyncClient,
    server_address: str,
    validator_ss58_address: str,
    miner_ss58_address: str,
    keypair: Keypair,
    endpoint: str,
    # payload: dict[str, Any],
    payload: Payload,
    timeout: float = QUERY_TIMEOUT,
) -> httpx.Response:
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

    response = await httpx_client.post(
        data=payload.data,
        files=files,
        timeout=timeout,
        headers=headers,
        url=server_address + endpoint,
    )
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
    timeout: float = QUERY_TIMEOUT,
) -> httpx.Response:
    headers = _get_headers(symmetric_key_uuid, validator_ss58_address)
    response = await httpx_client.get(
        url=server_address + endpoint,
        headers=headers,
        timeout=timeout,
    )
    return response
