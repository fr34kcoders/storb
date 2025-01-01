from binascii import unhexlify
from typing import Union

import bittensor as bt
from pydantic import BaseModel
from substrateinterface import Keypair

from storage_subnet.utils.piece import PieceType


class ChunkMessage(BaseModel):
    """Structure for a chunk message to be signed and verified."""

    chunk_hash: str
    validator_id: int
    piece_hashes: list[str]
    chunk_idx: int
    k: int
    m: int
    chunk_size: int
    padlen: int
    original_chunk_size: int


class TrackerMessage(BaseModel):
    """Structure for a tracker message to be signed and verified."""

    infohash: str
    validator_id: int
    filename: str
    length: int
    chunk_length: int
    chunk_count: int
    chunk_hashes: list[str]
    creation_timestamp: str


class PieceMessage(BaseModel):
    piece_hash: str
    miner_id: int
    chunk_idx: int
    piece_idx: int
    piece_type: PieceType


MessageType = Union[ChunkMessage, TrackerMessage, PieceMessage]


def sign_message(message: MessageType, wallet) -> str:
    """Sign a message with the wallet's hotkey.

    Parameters
    ----------
    message : bytes
        The message in utf-8 encoding to be signed.
    wallet : object
        The wallet containing the hotkey for signing.

    Returns
    -------
    str
        The signature of the provided message.
    """

    message = message.model_dump_json()
    bt.logging.debug(f"Signing message: {message}")
    keypair = wallet.hotkey
    signature = keypair.sign(message).hex()
    bt.logging.debug(f"Signature: {signature}")
    return signature


def verify_message(
    metagraph, message: MessageType, signature: str, signer: int
) -> bool:
    """Verify the signature of a message against a signer.

    Parameters
    ----------
    metagraph : object
        The metagraph containing hotkey information.
    message : str
        The message in utf-8 encoding whose signature needs to be verified.
    signature : str
        The cryptographic signature to be verified.
    signer : int
        The UID of the signer whose hotkey will be used for verification.

    Returns
    -------
    bool
        True if the signature is valid, False otherwise.

    Raises
    ------
    AssertionError
        If the metagraph is None or the signer UID is out of bounds.
    """

    assert metagraph is not None, "Metagraph must not be None."
    assert signer < len(metagraph.hotkeys), "Signer UID is out of bounds."

    message: str = message.model_dump_json()
    signature = unhexlify(signature.encode())

    signer_hotkey = metagraph.hotkeys[signer]
    keypair = Keypair(ss58_address=signer_hotkey, ss58_format=42)
    bt.logging.debug(f"Verifying message: {message}")
    result = keypair.verify(message, signature)
    bt.logging.debug(f"Verification result: {result}")
    return result
