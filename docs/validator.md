# Running a Validator

## Organic Validator

Organic validators have the ability to serve as gateways to the storage subnet, allowing them to store and retrieve files.

### Testnet

#### Setting up database

Ensure you have [dbmate](https://github.com/amacneil/dbmate) installed. Then run:

```bash
dbmate --url "sqlite:validator_database.db" up
```

#### Running

```bash
python neurons/validator.py --netuid 269 --subtensor.network test --wallet.name VALIDATOR_WALLET --wallet.hotkey VALIDATOR_HOTKEY --logging.debug --organic True --neuron.sync_frequency 60 --axon.port AXON_PORT --dht.port 6945
```

## Synthetic (Challenge) Validator

COMING VERY SOON - see: https://github.com/fr34kcoders/storb/pull/20
