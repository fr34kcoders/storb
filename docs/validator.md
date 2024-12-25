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
python neurons/validator.py --netuid 269 --subtensor.network test --wallet.name VALIDATOR_WALLET --wallet.hotkey VALIDATOR_HOTKEY --logging.debug --organic True --neuron.epoch_length 5 --axon.port AXON_PORT --dht.port 6945
```

### Selling your bandwidth

#### Managing access

To manage access to the your api server and sell access to anyone you like, using the storb cli tool is the easiest way.

```bash
storb --help
```

Shows all the commands and should give self explanatory instructions.

You can also do

```bash
storb some-command --help
```

To get more info about that command!

#### Examples

For example:

```bash
storb create-key --balance 10 --rate-limit-per-minute 60 --name test
```
Creates a test key with a balance of 10 (which corresponds to 10 requests), a rate limit of 60 requests per minute = 1/s, and a name 'test'.

Now you can do:
```bash
storb list-keys
```
To see the API key. Give / sell this access to whoever you want to have access to your API server to query the network organically.

#### Usage
Uploading a file example:
```
curl -X POST http://IP:API_PORT/store/ -H 'Authorization: Bearer API_KEY' -F "file=@FILE_LOCATION"
```
After uploading a file, you'll receive an infohash:
```
{"infohash":"d6ebcd55e3999c1301032ab4c99c573560cf674eef45862d11e0345681c1afce"}
```

This can be used to obtain retrieve the uploaded file:
```
curl -o downloaded_file -X GET http://IP:API_PORT/retrieve/?infohash=97d58f9e03da4062eed76e7724898065ded045bcccde7c1c07a1db4627bdfb66 -H 'Authorization: Bearer API_KEY'
```

## Synthetic (Challenge) Validator

COMING VERY SOON - see: https://github.com/fr34kcoders/storb/pull/20
