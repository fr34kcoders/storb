# Running a Validator

Validators play a crucial role in the Storb network by serving as gateways to the storage subnet. They handle the storage and retrieval of files, ensuring data integrity and availability.

## Types of Validators

- **Organic Validator:** Directly participates in the network by storing and retrieving files.
- **Synthetic (Challenge) Validator:** *(Coming Soon)* Designed for specific challenge scenarios where they verify that piece are stored by the designated miners. [Learn more](https://github.com/fr34kcoders/storb/pull/20)

---

## Organic Validator

Organic validators are the backbone of the Storb storage subnet, responsible for managing file storage and retrieval operations.

### Testnet Setup

1. **Activate Your Virtual Environment**

   Ensure you are within the virtual environment (`venv`) that you set up earlier:

   ```sh
   source path/to/your/venv/bin/activate
   ```

2. **Setting Up the Database**

    Initialize and migrate the validator database using `dbmate`:

    ```bash
    dbmate --url "sqlite:validator_database.db" up
    ```

3. **Running the Validator**

    Execute the validator with the appropriate configuration:

    ```sh
    NETUID=1 \
    WALLET_NAME=validator \
    HOTKEY_NAME=default \
    SUBTENSOR_NETWORK=<subtensor_network> \
    SUBTENSOR_ADDRESS=<subtensor_address> \
    MIN_STAKE_THRESHOLD=-1 \
    python storb/validator \
    --netuid 1 \
    --subtensor.network <subtensor_network> \
    --subtensor.address <subtensor_address> \
    --wallet_name validator \
    --hotkey_name default \
    --dht.port <port> \
    --external_ip <ip> \
    --api_port <port> \
    --db_dir vali_database.db
    ```

4. **Posting Your IP to the Chain (First-Time Setup)**

   If this is your first time running the miner, you need to post your IP address to the chain. Append the `--post_ip` flag to the command in the previous step.

**Running Multiple Nodes on the Same Machine**

   If you are running a miner on the same machine as a validator, ensure that `dht.port` and `api_port` are different to avoid conflicts. For example, if the miner uses port `4520`, you might set the validator's DHT port to `4521`:

   ```sh
   --dht.port 4521 \
   --api_port 4520
   ```
