import base64
import hashlib
import hmac
import random
from typing import Optional

import gmpy2
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.utils import int_to_bytes
from pydantic import BaseModel, field_serializer, field_validator

from storb.constants import DEFAULT_RSA_KEY_SIZE, G_CANDIDATE_RETRY, S_CANDIDATE_RETRY
from storb.util.logging import get_logger

logger = get_logger(__name__)


class APDPError(Exception):
    """Custom exception for APDP-related errors."""

    pass


class CryptoUtils:
    """Utility class for cryptographic operations."""

    @staticmethod
    def generate_rsa_private_key(key_size: int) -> rsa.RSAPrivateKey:
        """Generate an RSA private key.

        Parameters
        ----------
        key_size: int
            Size of the RSA key

        Returns
        -------
        rsa.RSAPrivateKey
            Generated RSA private key
        """

        return rsa.generate_private_key(public_exponent=65537, key_size=key_size)

    @staticmethod
    def full_domain_hash(rsa_key: rsa.RSAPrivateKey, data: bytes) -> int:
        """Compute the full domain hash of a given data block.

        Parameters
        ----------
        rsa_key: rsa.RSAPrivateKey
            RSA private key
        data: bytes
            Data block to hash

        Raises
        ------
        APDPError
            If the RSA key or data is invalid

        Returns
        -------
        int
            Full domain hash of the data block
        """

        if rsa_key is None or data is None:
            raise APDPError(
                "Invalid parameters for full_domain_hash. RSA key or data is None."
            )
        hashed = hashlib.sha256(data).digest()
        return int.from_bytes(hashed, "big") % rsa_key.public_key().public_numbers().n

    @staticmethod
    def prf(key: bytes, input_int: int, out_len=16) -> bytes:
        """Psuedo-random function using HMAC-SHA256

        Parameters
        ----------
        key: bytes
            Key used for encryption
        input_int: int
            Input integer to encrypt
        out_len: int
            Length of output block

        Returns
        -------
        bytes
            Encrypted block
        """

        if not key or len(key) == 0:
            raise APDPError("Invalid key for PRF")
        block = int_to_bytes(input_int, out_len)
        return hmac.digest(key, block, hashlib.sha256)


class APDPKey(BaseModel):
    """Holds the cryptographic parameters for single-block APDP. Using Pydantic to store/serialize them.
    Attributes
    ----------
    rsa: Optional[RSAPrivateKey]
        An RSA Private Key
    g: Optional[int]
        A generator for the group Z*_n (RSA modulus)
    prf_key: Optional[bytes]
        A key for the PRF function
    """

    rsa: Optional[RSAPrivateKey] = None
    g: Optional[int] = None
    prf_key: Optional[bytes] = None

    class Config:
        """Pydantic configuration for APDPKey."""

        arbitrary_types_allowed = True

    def generate(self, rsa_bits=DEFAULT_RSA_KEY_SIZE):
        """Generate RSA key, generator, and PRF key.

        Parameters
        ----------
        rsa_bits: int
            Size of the RSA key

        Raises
        ------
        APDPError
            If the RSA key size is invalid or keys cannot be generated
        """

        if rsa_bits <= 0:
            raise APDPError("Invalid RSA key size.")

        self.rsa = rsa.generate_private_key(public_exponent=65537, key_size=rsa_bits)
        if self.rsa is None:
            raise APDPError("Failed to generate RSA key.")
        n = self.rsa.public_key().public_numbers().n

        g_candidate = None
        for _ in range(G_CANDIDATE_RETRY):
            candidate = random.randint(2, n - 2)
            temp_val = gmpy2.powmod(gmpy2.mpz(candidate), 2, n)
            temp_val = int(temp_val)
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


class APDPTag(BaseModel):
    """Data model for APDP tag."""

    index: int
    tag_value: int
    prf_value: bytes

    @field_serializer("prf_value")
    def serialize_prf_value(cls, prf_value: bytes) -> str:
        """Serialize PRF value to base64.

        Parameters
        ----------
        prf_value: bytes
            PRF value to serialize

        Returns
        -------
        str
            Base64-encoded PRF value
        """

        return base64.b64encode(prf_value).decode("utf-8")

    @field_validator("prf_value", mode="before")
    def deserialize_prf_value(cls, value):
        """Deserialize PRF value from base64."""

        if isinstance(value, str):
            try:
                decoded_value = base64.b64decode(value)
                return decoded_value
            except Exception:
                raise ValueError("Invalid base64 for prf_value")
        return value


def generate_tag(data: bytes, keys: bytes, prf_key: bytes, g: int) -> APDPTag:
    """Generate a tag for a given data block.

    Parameters
    ----------
    data: bytes
        Data block to generate tag for

    Returns
    -------
    APDPTag
        Generated tag
    """
    key = serialization.load_pem_private_key(keys, password=None)

    if not data:
        raise APDPError("No data to generate tag.")

    # RSA parameters
    n = key.public_key().public_numbers().n

    # Convert entire data to integer mod n (RSA Public Modulus)
    block_int = int.from_bytes(data, "big") % n

    prf_value = CryptoUtils.prf(prf_key, 0)
    fdh_hash = CryptoUtils.full_domain_hash(key, prf_value)

    logger.debug(f"FDH hash: {fdh_hash}, PRF value: {prf_value}")

    base = (fdh_hash * (g, gmpy2.mpz(block_int), n)) % n
    d = key.private_numbers().d

    try:
        tag_value = gmpy2.powmod(gmpy2.mpz(base), gmpy2.mpz(d), gmpy2.mpz(n))
    except ValueError:
        raise APDPError("Failed to compute tag value.")

    return APDPTag(index=0, tag_value=tag_value, prf_value=prf_value)


class Challenge(BaseModel):
    """Data model for APDP challenge."""

    tag: APDPTag
    prp_key: bytes
    prf_key: bytes
    s: int
    g_s: int

    @field_serializer("prp_key")
    def serialize_prp_key(cls, prp_key: bytes) -> str:
        """Serialize PRP key to base64.

        Parameters
        ----------
        prp_key: bytes
            PRP key to serialize

        Returns
        -------
        str
            Base64-encoded PRP key
        """

        if isinstance(prp_key, bytes):
            new_key = base64.b64encode(prp_key).decode("utf-8")
            return new_key
        return prp_key

    @field_serializer("prf_key")
    def serialize_prf_key(cls, prf_key: bytes) -> str:
        """Serialize PRF key to base64.

        Parameters
        ----------
        prf_key: bytes
            PRF key to serialize

        Returns
        -------
        str
            Base64-encoded PRF key
        """

        if isinstance(prf_key, bytes):
            new_key = base64.b64encode(prf_key).decode("utf-8")
            return new_key
        return prf_key

    @field_validator("prf_key", mode="before")
    def deserialize_prf_key(cls, value):
        """Deserialize PRF key from base64."""

        if isinstance(value, str):
            try:
                decoded_value = base64.b64decode(value)
                return decoded_value
            except Exception:
                raise ValueError("Invalid base64 for prf_key")
        return value

    @field_validator("prp_key", mode="before")
    def deserialize_prp_key(cls, value):
        """Deserialize PRP key from base64."""

        if isinstance(value, str):
            try:
                decoded_value = base64.b64decode(value)
                return decoded_value
            except Exception:
                raise ValueError("Invalid base64 for prp_key")
        return value


class Proof(BaseModel):
    """Data model for APDP proof."""

    tag_value: int
    block_value: int
    hashed_result: str


class ChallengeSystem:
    """Main class for APDP challenge system."""

    def __init__(self):
        """Initialize the APDP challenge system."""

        self.key = APDPKey()

        logger.debug("Initialized APDP system.")

    def initialize_keys(self, rsa_bits=DEFAULT_RSA_KEY_SIZE):
        """Initialize keys for the APDP system.

        Parameters
        ----------
        rsa_bits: int
            Size of the RSA key
        """

        assert self.key is not None
        if rsa_bits <= 0:
            raise APDPError("Invalid RSA key size.")
        self.key.generate(rsa_bits)

    def generate_tag(self, data: bytes) -> APDPTag:
        """Generate a tag for a given data block.

        Parameters
        ----------
        data: bytes
            Data block to generate tag for

        Returns
        -------
        APDPTag
            Generated tag
        """

        if self.key.rsa is None or self.key.g is None or self.key.prf_key is None:
            raise APDPError(
                "Key values are not initialized. Call initialize_keys first."
            )
        if not data:
            raise APDPError("No data to generate tag.")

        # RSA parameters
        n = gmpy2.mpz(
            self.key.rsa.public_key().public_numbers().n
        )  # Use gmpy2's mpz for n
        block_int = (
            gmpy2.mpz(int.from_bytes(data, "big")) % n
        )  # Convert data to gmpy2.mpz mod n

        prf_value = CryptoUtils.prf(self.key.prf_key, 0)
        fdh_hash = gmpy2.mpz(
            CryptoUtils.full_domain_hash(self.key.rsa, prf_value)
        )  # Ensure gmpy2 type

        logger.debug(f"FDH hash: {fdh_hash}, PRF value: {prf_value}")

        g = gmpy2.mpz(self.key.g)  # Convert g to gmpy2.mpz
        base = (
            fdh_hash * gmpy2.powmod(g, block_int, n)
        ) % n  # Use gmpy2.powmod for modular exponentiation

        d = gmpy2.mpz(
            self.key.rsa.private_numbers().d
        )  # Ensure private exponent is gmpy2.mpz

        try:
            tag_value = gmpy2.powmod(base, d, n)  # Modular exponentiation for tag value
        except ValueError:
            raise APDPError("Failed to compute tag value.")

        return APDPTag(index=0, tag_value=int(tag_value), prf_value=prf_value)

    def issue_challenge(self, tag: APDPTag) -> Challenge:
        """Issue a challenge for a given tag.

        Parameters
        ----------
        tag: APDPTag
            Tag to issue challenge for

        Returns
        -------
        Challenge
            Issued challenge
        """

        if self.key.rsa is None or self.key.g is None:
            raise APDPError(
                "Key values are not initialized. Call initialize_keys first."
            )

        n = gmpy2.mpz(self.key.rsa.public_key().public_numbers().n)
        s = random.randint(2, n - 1)

        attempt_count = 0
        while gmpy2.gcd(s, n) != 1:
            s = random.randint(2, n - 1)
            attempt_count += 1
            if attempt_count > S_CANDIDATE_RETRY:
                raise APDPError("Failed to find suitable s in Z*_n")

        g_s = gmpy2.powmod(gmpy2.mpz(self.key.g), s, n)
        prp_key = Fernet.generate_key()
        prf_key = Fernet.generate_key()

        tag = APDPTag.model_validate_json(tag)

        g_s = int(g_s)
        return Challenge(s=s, g_s=g_s, prf_key=prf_key, prp_key=prp_key, tag=tag)

    def generate_proof(
        self, data: bytes, tag: APDPTag, challenge: Challenge, n: int
    ) -> Proof:
        """Generate a proof for a given data block, tag, and challenge.

        Parameters
        ----------
        data: bytes
            Data block to generate proof for
        tag: APDPTag
            Tag for the data block
        challenge: Challenge
            Challenge for the tag
        n: int
            RSA modulus

        Returns
        -------
        Proof
            Generated proof
        """

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

        n = gmpy2.mpz(n)
        coefficient = gmpy2.mpz(coefficient)

        aggregated_tag = gmpy2.powmod(gmpy2.mpz(tag.tag_value), coefficient, n)
        aggregated_tag = int(aggregated_tag)
        aggregated_blocks = coefficient * block_int
        aggregated_blocks = gmpy2.mpz(aggregated_blocks)
        aggregated_blocks = int(aggregated_blocks)

        logger.debug(
            f"Aggregated tag: {aggregated_tag}, aggregated blocks: {aggregated_blocks}"
        )

        rho = gmpy2.powmod(gmpy2.mpz(challenge.g_s), aggregated_blocks, n)
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
        """Verify a proof given a challenge, tag, and proof.

        Parameters
        ----------
        proof: Proof
            Proof to verify
        challenge: Challenge
            Challenge for the proof
        tag: APDPTag
            Tag for the proof
        n: int
            RSA modulus
        e: int
            RSA public exponent

        Returns
        -------
        bool
            True if proof is valid, False otherwise
        """

        if proof is None or challenge is None or tag is None:
            raise APDPError("Invalid proof, challenge, or tag.")
        if self.key.rsa is None:
            raise APDPError("Keys not initialized.")

        rsa_key = self.key.rsa

        e = gmpy2.mpz(e)
        n = gmpy2.mpz(n)

        try:
            tau = gmpy2.powmod(gmpy2.mpz(proof.tag_value), e, n)
        except ValueError:
            raise APDPError("Failed to compute tau in verification.")

        logger.debug(f"Computed tau: {tau}")
        prf_result = CryptoUtils.prf(challenge.prf_key, 0)
        coefficient = int.from_bytes(prf_result, "big") % n
        coefficient = gmpy2.mpz(coefficient)
        fdh_hash = CryptoUtils.full_domain_hash(rsa_key, tag.prf_value)
        denominator = gmpy2.powmod(gmpy2.mpz(fdh_hash), coefficient, n) % n
        try:
            denominator_inv = gmpy2.powmod(gmpy2.mpz(denominator), -1, n)
        except ValueError:
            raise APDPError("Failed to invert denominator modulo n.")

        tau = (tau * denominator_inv) % n
        tau_s = gmpy2.powmod(tau, challenge.s, n)

        expected_hash = hashlib.sha256(int_to_bytes(tau_s)).digest()
        expected_hash = base64.b64encode(expected_hash).decode("utf-8")
        # convert expected hash to base64
        logger.debug(
            f"Expected hash: {expected_hash} vs. Proof hash: {proof.hashed_result}"
        )
        return expected_hash == proof.hashed_result
