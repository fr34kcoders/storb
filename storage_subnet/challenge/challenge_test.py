import os

import pytest

from storage_subnet.challenge import APDPError, ChallengeSystem


@pytest.fixture
def challenge_system():
    return ChallengeSystem(block_size=256)


def test_no_keys_initialized(challenge_system):
    data = os.urandom(1024)

    with pytest.raises(APDPError):
        challenge_system.generate_tags(data)

    with pytest.raises(APDPError):
        challenge_system.issue_challenge(10)


def test_invalid_key_size(challenge_system):
    with pytest.raises(APDPError):
        challenge_system.initialize_keys(rsa_bits=0)


def test_empty_data(challenge_system):
    challenge_system.initialize_keys()

    with pytest.raises(APDPError):
        challenge_system.generate_tags(b"")


def test_uninitialized_proof(challenge_system):
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tags = challenge_system.generate_tags(data)

    with pytest.raises(APDPError):
        challenge_system.generate_proof(data, tags, None)


def test_incompatible_challenge(challenge_system):
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tags = challenge_system.generate_tags(data)
    challenge = challenge_system.issue_challenge(len(tags))

    challenge.num_blocks = len(tags) + 5

    with pytest.raises(APDPError):
        challenge_system.generate_proof(data, tags, challenge)


def test_challenge(challenge_system):
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tags = challenge_system.generate_tags(data)
    challenge = challenge_system.issue_challenge(len(tags))
    proof = challenge_system.generate_proof(data, tags, challenge)

    verification_result = challenge_system.verify_proof(proof, challenge, tags)
    print(verification_result)
    assert verification_result, "Proof verification should succeed."


def test_verification_failure(challenge_system):
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tags = challenge_system.generate_tags(data)
    challenge = challenge_system.issue_challenge(len(tags))
    proof = challenge_system.generate_proof(data, tags, challenge)

    proof.aggregated_tag += 1

    assert not challenge_system.verify_proof(
        proof, challenge, tags
    ), "Proof verification should fail if proof is tampered."


def test_verification_failure_invalid_data(challenge_system):
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tags = challenge_system.generate_tags(data)
    challenge = challenge_system.issue_challenge(len(tags))
    tampered_data = os.urandom(1024)
    proof = challenge_system.generate_proof(tampered_data, tags, challenge)

    assert not challenge_system.verify_proof(
        proof, challenge, tags
    ), "Proof verification should fail if data is tampered."


def test_invalid_prp_prf_params(challenge_system):
    challenge_system.initialize_keys()
    data = os.urandom(1024)

    tags = challenge_system.generate_tags(data)
    challenge = challenge_system.issue_challenge(len(tags))

    challenge.prp_key = b""

    with pytest.raises(APDPError):
        challenge_system.generate_proof(data, tags, challenge)
