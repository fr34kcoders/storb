# Storb - An Overview

# TLDR
Basically, the subnet can be used as shown below:
![overview](../assets/overview.png)

# Scoring Mechanism
![scoring](../assets/weight-scoring.png)
Scoring is made up of multiple components:
- **Latency**: Miners are scored based on how quickly they respond to storage and retrieval requests.
- **Response Rate**: Reliable miners are the name of the game. The less a miner responds to storage and retrieval requests the lower it is scored.
- **PDP Challenge**: The subnet uses an efficient proof of data possession technique inspired by [Ateniese et al.](https://dl.acm.org/doi/10.1145/1315245.1315318) to ensure miners actually store the data they are asked to keep.

Please read [neurons/validator.py](../neurons/validator.py) for more information on how the scoring mechanism works.

# Chunking and Piecing
Files are split into erasure-coded chunks, and subsequently split into pieces and stored across various miners for redundancy
![chunk](../assets/chunk.png)

# DHT for File Metadata
File metadata - which is useful for querying miners for pieces, and, eventually, reconstructing files - is replicated and stored across neurons in the subnet in the form of a DHT.
![metadata](../assets/metadata.png)