# Running a Validator

## Organic Validator
Organic validators have the ability to serve as gateways to the storage subnet, allowing them to store and retrieve files.

### Testnet
#### Setting up database
Ensure you have [dbmate](https://github.com/amacneil/dbmate) installed. Then run:
```
dbmate --url "sqlite:validator_database.db" up
```
#### Running
```
python neurons/validator.py --netuid 269 --subtensor.network test --wallet.name VALIDATOR_WALLET --wallet.hotkey VALIDATOR_HOTKEY --logging.debug --organic True --neuron.epoch_length 5 --axon.port AXON_PORT --dht.port 6945
```

## Synthetic Validator
COMING VERY SOON - see: https://github.com/fr35kcoders/storb/pull/20