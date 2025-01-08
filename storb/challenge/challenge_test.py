import os

import pytest

from storb.challenge import (
    APDPError,
    ChallengeSystem,
)


@pytest.fixture
def challenge_system():
    """
    Returns a new instance of the single-block system.
    No need to specify block_size since we don't split data anymore.
    """
    return ChallengeSystem()


def test_no_keys_initialized(challenge_system):
    """
    Verify that calling methods requiring keys before initialization raises errors.
    """
    data = os.urandom(1024)

    # generate_tag should fail if keys not initialized
    with pytest.raises(APDPError):
        tag = challenge_system.generate_tag(data)
        challenge_system.issue_challenge(tag)


def test_invalid_key_size(challenge_system):
    """
    Initializing keys with rsa_bits=0 should raise APDPError.
    """
    with pytest.raises(APDPError):
        challenge_system.initialize_keys(rsa_bits=0)


def test_empty_data(challenge_system):
    """
    Generating a tag for empty data should fail.
    """
    challenge_system.initialize_keys()

    with pytest.raises(APDPError):
        challenge_system.generate_tag(b"")


def test_uninitialized_proof(challenge_system):
    """
    Generating a proof with a None challenge should raise APDPError.
    """
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    # Generate a valid tag
    tag = challenge_system.generate_tag(data)

    # Attempt to generate proof with challenge=None
    with pytest.raises(APDPError):
        challenge_system.generate_proof(data, tag, None)


def test_challenge(challenge_system):
    """
    Happy-path test: generate tag, issue challenge, create proof, verify success.
    """
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    # Single-block tag
    tag = challenge_system.generate_tag(data)

    # Single-block challenge
    challenge = challenge_system.issue_challenge(tag)

    # Generate proof
    proof = challenge_system.generate_proof(data, tag, challenge)

    # Verify proof
    verification_result = challenge_system.verify_proof(proof, challenge, tag)
    assert verification_result, "Proof verification should succeed for correct data."


def test_verification_failure(challenge_system):
    """
    Tamper with the proof to ensure verification fails.
    """
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tag = challenge_system.generate_tag(data)
    challenge = challenge_system.issue_challenge(tag)
    proof = challenge_system.generate_proof(data, tag, challenge)

    # Tamper with the proof (e.g., increment the tag_value)
    proof.tag_value += 1

    assert not challenge_system.verify_proof(
        proof, challenge, tag
    ), "Verification should fail after tampering with the proof."


def test_verification_failure_invalid_data(challenge_system):
    """
    Use different data (tampered_data) to generate the proof and verify with the original tag.
    """
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tag = challenge_system.generate_tag(data)
    challenge = challenge_system.issue_challenge(tag)

    # Tamper with data before generating proof
    tampered_data = os.urandom(1024)
    proof = challenge_system.generate_proof(tampered_data, tag, challenge)

    # Verify with the original tag and challenge => should fail
    assert not challenge_system.verify_proof(
        proof, challenge, tag
    ), "Proof verification should fail if data is tampered."
