# Challenge System Documentation

## Overview

The **Challenge System** in Storb is designed to implement a [**Provable Data Possession (PDP)** protocol](https://dl.acm.org/doi/10.1145/1315245.1315318). This system enables a **validator** to verify that a **miner** is correctly storing specific data blocks without requiring access to the entire data. The system leverages cryptographic techniques to ensure data integrity and storage reliability in a secure and efficient manner.

---

## Key Components

### 1. **APDPKey**

The `APDPKey` class manages the cryptographic parameters essential for the PDP protocol.

- **Attributes:**
  - `rsa`: An RSA private key (`RSAPrivateKey`) used for generating and verifying tags.
  - `g`: An integer representing a generator in the multiplicative group $\mathbb{Z}_n^*$.
  - `prf_key`: A byte string used as a key for the Pseudo-Random Function (PRF).

- **Methods:**
  - `generate(rsa_bits)`: Initializes the RSA key, selects a suitable generator `g`, and generates the `prf_key`.
  - `clear()`: Clears all cryptographic material from the key instance.

### 2. **APDPTag**

The `APDPTag` class represents a cryptographic tag associated with a specific data block.

- **Attributes:**
  - `index`: An integer identifier for the data block (e.g., 0 for single-block scenarios).
  - `tag_value`: An integer representing the cryptographic tag of the data block.
  - `prf_value`: A byte string derived from the PRF, adding randomness to the tag.

- **Serialization:**
  - `prf_value` is serialized and deserialized using Base64 encoding to ensure safe transmission and storage.

### 3. **Challenge**

The `Challenge` class encapsulates the parameters sent by the validator to the miner to initiate a verification request.

- **Attributes:**
  - `tag`: An instance of `APDPTag` corresponding to the data being challenged.
  - `prp_key`: A byte string serving as a key for the Pseudo-Random Permutation (PRP).
  - `prf_key`: A byte string serving as a key for the PRF.
  - `s`: An integer randomly selected to introduce variability in the challenge.
  - `g_s`: An integer calculated as $g^s \mod n$, where `g` is the generator and `n` is the RSA modulus.

- **Serialization:**
  - `prp_key` and `prf_key` are serialized and deserialized using Base64 encoding.

### 4. **Proof**

The `Proof` class represents the miner’s response to a challenge, demonstrating possession of the data.

- **Attributes:**
  - `tag_value`: An integer derived from the original tag value, modified based on the challenge.
  - `block_value`: An integer representing the aggregated data block value influenced by the challenge.
  - `hashed_result`: A Base64-encoded string containing the SHA-256 hash of a critical computation, ensuring proof integrity.

### 5. **ChallengeSystem**

The `ChallengeSystem` class orchestrates the entire PDP process, managing key operations such as key initialization, tag generation, challenge issuance, proof generation, and proof verification.

- **Attributes:**
  - `key`: An instance of `APDPKey` containing all necessary cryptographic parameters.
  - `challenges`: A dictionary storing issued challenges keyed by their unique identifiers.
  - `challenge_queue`: An asynchronous queue managing incoming challenges for processing.

- **Methods:**
  - `initialize_keys(rsa_bits)`: Initializes the cryptographic keys using the specified RSA key size.
  - `generate_tag(data)`: Creates an `APDPTag` for a given data block.
  - `issue_challenge(tag)`: Generates a `Challenge` based on the provided tag.
  - `generate_proof(data, tag, challenge, n)`: Produces a `Proof` in response to a challenge.
  - `verify_proof(proof, challenge, tag, n, e)`: Validates the received proof against the original challenge and tag.

---

## Workflow

### Step 1: **Key Initialization**

Initialize the cryptographic keys required for the PDP protocol.

```python
self.key = APDPKey()
self.key.generate(rsa_bits=DEFAULT_RSA_KEY_SIZE)
```

- **Process:**
  - Generates an RSA private key with the specified key size.
  - Selects a suitable generator `g` in $\mathbb{Z}_n$.
  - Generates a PRF key (`prf_key`) for pseudorandom number generation.

### Step 2: **Tag Generation**

Generate a cryptographic tag for a specific data block.

```python
tag = self.generate_tag(data)
```

- **Process:**
  - Converts the data block into an integer modulo the RSA modulus `n`.
  - Generates a PRF value (`prf_value`) using the `prf_key`.
  - Computes the tag value (`tag_value`) by combining the hashed data and generator operations, followed by exponentiation with the RSA private exponent.

### Step 3: **Issuing a Challenge**

Create and issue a challenge to a miner to verify data possession.

```python
challenge = self.issue_challenge(tag)
```

- **Process:**
  - Selects a random integer `s` ensuring $\gcd(s, n) = 1$.
  - Calculates $g_s = g^s \mod n$.
  - Generates ephemeral keys (`prp_key`, `prf_key`) for the challenge.
  - Constructs a `Challenge` object encapsulating the tag and challenge parameters.

### Step 4: **Generating a Proof**

The miner generates a proof in response to the received challenge.

```python
proof = self.generate_proof(data, tag, challenge, n)
```

- **Process:**
  - Derives a coefficient using the PRF with `prf_key`.
  - Adjusts the tag value by raising it to the derived coefficient modulo `n`.
  - Aggregates the data block value by multiplying it with the coefficient.
  - Computes rho and hashes the result to produce `hashed_result`.
  - Constructs a `Proof` object containing the modified tag value, aggregated block value, and hashed result.

### Step 5: **Verifying a Proof**

The validator verifies the proof submitted by the miner.

```python
is_valid = self.verify_proof(proof, challenge, tag, n, e)
```

- **Process:**
  - Recomputes `tau = (proof * tag_value^e) mod n`.
  - Eliminates the effect of the full domain hash (FDH) by applying the modular inverse of the FDH component.
  - Calculates $tau_s = \tau^s \mod n$ based on the challenge parameter `s`.
  - Hashes $tau_s$ and compares it with the `hashed_result` from the proof.
  - Returns `True` if the hashes match, indicating valid proof; otherwise, returns `False`.

---

## Asynchronous Challenge Workflow

The Challenge System operates asynchronously to support distributed and scalable environments. Below are the detailed workflows for both the **validator** and the **miner**.

### Validator Workflow

1. **Initiate Challenge**

   The validator selects a miner and a specific data piece to challenge.

   ```python
   await self.challenge_miner(miner_id, piece_id, tag)
   ```

   - **Process:**
     - Constructs a `Challenge` using `issue_challenge(tag)`.
     - Signs the challenge message to ensure authenticity.
     - Sets a deadline for the miner to respond.
     - Sends the signed challenge message to the selected miner.

2. **Verify Response**

   Upon receiving a proof from the miner, the validator verifies its authenticity and correctness.

   ```python
   self.verify_challenge(challenge_request)
   ```

   - **Process:**
     - Retrieves the corresponding `Challenge` using the challenge ID.
     - Validates the proof using `verify_proof(proof, challenge, tag, n, e)`.
     - Logs the result and updates the challenge status accordingly.

### Miner Workflow

1. **Receive Challenge**

   The miner acknowledges and queues the received challenge for processing.

   ```python
   await self.ack_challenge(request)
   ```

   - **Process:**
     - Validates the authenticity of the challenge (e.g., verifies the signature).
     - Parses and stores the challenge parameters.
     - Enqueues the challenge with its deadline for proof generation.

2. **Generate Proof**

   The miner processes the queued challenge by generating a corresponding proof.

   ```python
   proof = self.generate_proof(data, tag, challenge, n)
   ```

   - **Process:**
     - Retrieves the data block associated with the challenge.
     - Uses the `generate_proof` method to create a `Proof` object based on the challenge parameters.
     - Ensures the proof adheres to the protocol specifications.

3. **Send Proof**

   The miner submits the generated proof back to the validator for verification.

   - **Process:**
     - Constructs a `ProofResponse` containing the proof and related metadata.
     - Sends the `ProofResponse` to the validator's designated endpoint.
     - Waits for acknowledgment or further instructions.

### Processing Challenges

The system includes an asynchronous consumer that continuously processes incoming challenges.

```python
await self.consume_challenges()
```

- **Process:**
  - Continuously retrieves challenges from the `challenge_queue`.
  - Validates the deadline and ensures the challenge is still active.
  - Reads the required data block from storage.
  - Generates a proof using the stored data and challenge parameters.
  - Sends the proof back to the validator for verification.
  - Marks the challenge as completed or handles errors accordingly.
