import base64
import hashlib
import hmac
import os
import random
from math import gcd
from typing import Optional

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.utils import int_to_bytes
from pydantic import BaseModel, field_serializer, field_validator

from storb.util.logging import get_logger

logger = get_logger(__name__)

# Constants
DEFAULT_RSA_KEY_SIZE = 2048


class APDPError(Exception):
    """Custom exception for APDP-related errors."""

    pass


class CryptoUtils:
    """Utility class for cryptographic operations."""

    @staticmethod
    def generate_rsa_private_key(key_size: int) -> rsa.RSAPrivateKey:
        return rsa.generate_private_key(public_exponent=65537, key_size=key_size)

    @staticmethod
    def full_domain_hash(rsa_key: rsa.RSAPrivateKey, data: bytes) -> int:
        if rsa_key is None or data is None:
            raise APDPError("Invalid parameters for full_domain_hash")
        hashed = hashlib.sha256(data).digest()
        return int.from_bytes(hashed, "big") % rsa_key.public_key().public_numbers().n

    @staticmethod
    def prf(key: bytes, input_int: int, out_len=16) -> bytes:
        """
        Psuedo-random function using HMAC-SHA256

        Arguments:
            key (bytes): Key used for encryption
            input_int (int): Input integer to encrypt
            out_len (int): Length of output block

        Returns:
            bytes: Encrypted block
        """
        if not key or len(key) == 0:
            raise APDPError("Invalid key for PRF")
        block = int_to_bytes(input_int, out_len)
        return hmac.digest(key, block, hashlib.sha256)


class APDPKey(BaseModel):
    """
    Holds the cryptographic parameters for single-block APDP.
    Using Pydantic to store/serialize them.
    """

    rsa: Optional[RSAPrivateKey] = None
    g: Optional[int] = None
    prf_key: Optional[bytes] = None

    class Config:
        arbitrary_types_allowed = True

    def generate(self, rsa_bits=DEFAULT_RSA_KEY_SIZE):
        if rsa_bits <= 0:
            raise APDPError("Invalid RSA key size.")

        self.rsa = rsa.generate_private_key(public_exponent=65537, key_size=rsa_bits)
        if self.rsa is None:
            raise APDPError("Failed to generate RSA key.")
        n = self.rsa.public_key().public_numbers().n

        g_candidate = None
        for _ in range(1000):
            candidate = random.randint(2, n - 2)
            temp_val = pow(candidate, 2, n)
            if temp_val not in (0, 1):
                g_candidate = temp_val
                break

        if g_candidate is None:
            raise APDPError("Failed to find suitable generator g.")
        self.g = g_candidate

        self.prf_key = Fernet.generate_key()
        if not self.prf_key:
            raise APDPError("Failed to generate PRF key")

    def clear(self):
        """Clear all stored key material."""
        self.rsa = None
        self.g = None
        self.prf_key = None

    # def get_private_key_obj(self) -> Optional[RSAPrivateKey]:
    #     """Helper method if you prefer a clearer name."""
    #     return self._rsa_obj


class APDPTag(BaseModel):
    index: int
    tag_value: int
    prf_value: bytes

    @field_serializer("prf_value")
    def serialize_prf_value(self, prf_value: bytes) -> str:
        return base64.b64encode(prf_value).decode("utf-8")

    @field_validator("prf_value", mode="before")
    def deserialize_prf_value(cls, value):
        if isinstance(value, str):
            try:
                decoded_value = base64.b64decode(value)
                return decoded_value
            except Exception as e:
                raise ValueError("Invalid base64 for prf_value")
        return value


class Challenge(BaseModel):
    tag: APDPTag
    prp_key: bytes
    prf_key: bytes
    s: int
    g_s: int

    @field_serializer("prp_key")
    def serialize_prp_key(self, prp_key: bytes) -> str:
        logger.critical(f"Serializing prp_key: {prp_key}")
        if isinstance(prp_key, bytes):
            new_key = base64.b64encode(prp_key).decode("utf-8")
            logger.critical(f"Returning new_key: {new_key}")
            return new_key
        return prp_key

    @field_serializer("prf_key")
    def serialize_prf_key(self, prf_key: bytes) -> str:
        logger.critical(f"Serializing prf_key: {prf_key}")
        if isinstance(prf_key, bytes):
            new_key = base64.b64encode(prf_key).decode("utf-8")
            logger.critical(f"Returning new_key: {new_key}")
            return new_key
        return prf_key

    @field_validator("prf_key", mode="before")
    def deserialize_prf_key(cls, value):
        logger.critical(f"Deserializing prf_key: {value}")
        if isinstance(value, str):
            try:
                decoded_value = base64.b64decode(value)
                logger.critical(f"Decoded prf_key: {decoded_value}")
                return decoded_value
            except Exception as e:
                logger.critical(f"Error decoding prf_key: {e}")
                raise ValueError("Invalid base64 for prf_key")
        return value

    @field_validator("prp_key", mode="before")
    def deserialize_prp_key(cls, value):
        logger.critical(f"Deserializing prp_key: {value}")
        if isinstance(value, str):
            try:
                decoded_value = base64.b64decode(value)
                logger.critical(f"Decoded prp_key: {decoded_value}")
                return decoded_value
            except Exception as e:
                logger.critical(f"Error decoding prp_key: {e}")
                raise ValueError("Invalid base64 for prp_key")
        return value


class Proof(BaseModel):
    tag_value: int
    block_value: int
    hashed_result: str


class ChallengeSystem:
    """Single-block APDP with Pydantic-based key and data models."""

    def __init__(self):
        self.key = APDPKey()
        logger.debug("Initialized APDP system.")

    def initialize_keys(self, rsa_bits=DEFAULT_RSA_KEY_SIZE):
        assert self.key is not None
        if rsa_bits <= 0:
            raise APDPError("Invalid RSA key size.")
        self.key.generate(rsa_bits)

    def generate_tag(self, data: bytes) -> APDPTag:
        if self.key.rsa is None or self.key.g is None or self.key.prf_key is None:
            raise APDPError("Keys are not initialized.")
        if not data:
            raise APDPError("No data to generate tag.")

        # RSA parameters
        n = self.key.rsa.public_key().public_numbers().n
        p = self.key.rsa.private_numbers().p
        q = self.key.rsa.private_numbers().q
        phi = (p - 1) * (q - 1)
        if phi <= 0:
            raise APDPError("Invalid RSA parameters.")

        # Convert entire data to integer mod phi
        block_int = int.from_bytes(data, "big") % n

        prf_value = CryptoUtils.prf(self.key.prf_key, 0)
        fdh_hash = CryptoUtils.full_domain_hash(self.key.rsa, prf_value)

        logger.debug(f"FDH hash: {fdh_hash}, PRF value: {prf_value}")

        base = (fdh_hash * pow(self.key.g, block_int, n)) % n
        d = self.key.rsa.private_numbers().d

        try:
            tag_value = pow(base, d, n)
        except ValueError:
            raise APDPError("Failed to compute tag value.")

        return APDPTag(index=0, tag_value=tag_value, prf_value=prf_value)

    def issue_challenge(self, tag: APDPTag) -> Challenge:
        if self.key.rsa is None or self.key.g is None:
            raise APDPError("Keys are not initialized.")

        n = self.key.rsa.public_key().public_numbers().n
        s = random.randint(2, n - 1)

        attempt_count = 0
        while gcd(s, n) != 1:
            s = random.randint(2, n - 1)
            attempt_count += 1
            if attempt_count > 10000:
                raise APDPError("Failed to find suitable s in Z*_n")

        g_s = pow(self.key.g, s, n)
        prp_key = Fernet.generate_key()
        prf_key = Fernet.generate_key()

        tag = APDPTag.model_validate_json(tag)

        return Challenge(s=s, g_s=g_s, prf_key=prf_key, prp_key=prp_key, tag=tag)

    def generate_proof(
        self, data: bytes, tag: APDPTag, challenge: Challenge, n: int
    ) -> Proof:
        if not tag or not challenge:
            raise APDPError("Invalid tag or challenge for proof generation.")

        logger.debug(f"Generating proof for data: {data[:10]}...")

        logger.debug(f"RSA modulus: {n}")
        block_int = int.from_bytes(data, "big") % n

        prf_result = CryptoUtils.prf(challenge.prf_key, 0)
        coefficient = int.from_bytes(prf_result, "big") % n

        logger.debug(
            f"Block int: {block_int}, coefficient: {coefficient}, prf: {prf_result}"
        )

        aggregated_tag = pow(tag.tag_value, coefficient, n)
        aggregated_blocks = coefficient * block_int

        logger.debug(
            f"Aggregated tag: {aggregated_tag}, aggregated blocks: {aggregated_blocks}"
        )

        rho = pow(challenge.g_s, aggregated_blocks, n)
        hashed_result = hashlib.sha256(int_to_bytes(rho)).digest()

        logger.debug(f"Hashed result: {hashed_result}")
        hashed_result = base64.b64encode(hashed_result).decode("utf-8")

        return Proof(
            tag_value=aggregated_tag,
            block_value=aggregated_blocks,
            hashed_result=hashed_result,
        )

    def verify_proof(
        self, proof: Proof, challenge: Challenge, tag: APDPTag, n: int, e: int
    ) -> bool:
        if proof is None or challenge is None or tag is None:
            raise APDPError("Invalid proof, challenge, or tag.")
        if self.key.rsa is None:
            raise APDPError("Keys not initialized.")

        rsa_key = self.key.rsa

        try:
            tau = pow(proof.tag_value, e, n)
        except ValueError:
            raise APDPError("Failed to compute tau in verification.")

        logger.debug(f"Computed tau: {tau}")
        prf_result = CryptoUtils.prf(challenge.prf_key, 0)
        coefficient = int.from_bytes(prf_result, "big") % n
        fdh_hash = CryptoUtils.full_domain_hash(rsa_key, tag.prf_value)
        denominator = pow(fdh_hash, coefficient, n) % n
        try:
            denominator_inv = pow(denominator, -1, n)
        except ValueError:
            raise APDPError("Failed to invert denominator modulo n.")

        tau = (tau * denominator_inv) % n
        tau_s = pow(tau, challenge.s, n)

        expected_hash = hashlib.sha256(int_to_bytes(tau_s)).digest()
        expected_hash = base64.b64encode(expected_hash).decode("utf-8")
        # convert expected hash to base64
        logger.debug(
            f"Expected hash: {expected_hash} vs. Proof hash: {proof.hashed_result}"
        )
        return expected_hash == proof.hashed_result


def main():
    data = os.urandom(1024)
    system = ChallengeSystem()

    # The next line should pass, since we declare `_rsa_obj` as PrivateAttr:
    system.initialize_keys()

    # Test the entire flow
    tag = system.generate_tag(data)
    challenge = system.issue_challenge(tag)
    proof = system.generate_proof(data, tag, challenge)
    result = system.verify_proof(proof, challenge, tag)

    print("Verification:", "Success" if result else "Failure")


if __name__ == "__main__":
    main()
