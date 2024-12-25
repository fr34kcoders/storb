# Running a Miner

Miners are the backbone of Storb. They are responsible for storing and serving pieces (of files) to validators, and by proxy to the end user. 

### Testnet

#### Running

Within your venv that you set up earlier:

```sh
python neurons/miner.py --netuid 269 --subtensor.network test --wallet.name MINER_WALLET --wallet.hotkey MINER_HOTKEY --logging.debug --axon.port AXON_PORT
```
