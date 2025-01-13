# Running a Miner

Miners are the backbone of Storb. They are responsible for storing and serving pieces of files to validators, and by proxy, to the end users.

## Running a Miner

Follow these steps to run a miner on the Storb Testnet:

1. **Activate Your Virtual Environment**

   Ensure you are within the virtual environment (`venv`) that you set up earlier:

   ```sh
   source path/to/your/venv/bin/activate
   ```

2. **Run the Miner Command**

   Execute the following command, replacing `<subtensor_address>` with your actual subnet address and adjusting the `--external_ip` to your machine's external IP address:

   ```sh
   NETUID=1 \
   WALLET_NAME=miner \
   HOTKEY_NAME=default \
   SUBTENSOR_NETWORK=<subtensor_network> \
   SUBTENSOR_ADDRESS=<subtensor_address> \
   MIN_STAKE_THRESHOLD=-1 \
   python storb/miner \
     --netuid 269 \ # 269 on testnet
     --subtensor.network <subtensor_network> \
     --subtensor.address <subtensor_address> \
     --wallet_name miner \
     --hotkey_name default \
     --dht.port <port> \
     --external_ip <ip> \
     --api_port <port>
   ```

3. **Posting Your IP to the Chain (First-Time Setup)**

   If this is your first time running the miner, you need to post your IP address to the chain. Append the `--post_ip` flag to the command in the previous step.

**Running Multiple Node on the Same Machine**

   If you are running a miner on the same machine as a validator, ensure that `dht.port` and `api_port` are different to avoid conflicts. For example, if the validator uses port `4520`, you might set the miner's DHT port to `4521`:

   ```sh
   --dht.port 4521 \
   --api_port 4520
   ```
