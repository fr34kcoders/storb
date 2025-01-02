"""
Configuration options for Storb
"""

from argparse import ArgumentParser
from pathlib import Path

from dynaconf import Dynaconf

from storb.constants import NeuronType


class Config:
    """Configurations for Storb

    The CLI arguments correspond to the options in the TOML file.
    """

    def __init__(self, neuron_type: NeuronType):
        self.settings = Dynaconf(
            settings_files=["settings.toml", ".secrets.toml"],
        )
        self._parser = ArgumentParser()

        # Add arguments
        self.add_args()
        neuron_name = "base"
        match neuron_type:
            case NeuronType.Base:
                pass
            case NeuronType.Miner:
                self.add_miner_args()
                neuron_name = "miner"
            case NeuronType.Validator:
                self.add_validator_args()
                neuron_name = "validator"
            case _:
                raise ValueError(
                    f"The provided neuron_type ({self.settings.neuron_type}) is not valid."
                )

        options = self._parser.parse_args()
        self.add_full_path(options, neuron_name)
        self.settings.update(vars(options))

    @classmethod
    def add_full_path(cls, options, neuron_name: str):
        r"""Checks/validates the config namespace object."""
        base_path = (
            Path.home() / ".bittensor" / "neurons"
        )  # Update path as noted in the TODO
        full_path = (
            base_path
            / options.wallet_name
            / options.hotkey_name
            / f"netuid{options.netuid}"
            / neuron_name
        )
        print("full path:", full_path)
        options.full_path = full_path
        if not full_path.exists():
            full_path.mkdir(parents=True, exist_ok=True)

    def add_args(self):
        self._parser.add_argument(
            "--netuid", type=int, help="Subnet netuid", default=self.settings.netuid
        )

        self._parser.add_argument(
            "--external_ip",
            type=str,
            help="External IP",
            default=self.settings.external_ip,
        )

        self._parser.add_argument(
            "--api_port",
            type=int,
            help="API port for the node",
            default=self.settings.api_port,
        )

        self._parser.add_argument(
            "--post_ip",
            help="If you want to run a organic validator",
            action="store_true",
            default=self.settings.post_ip,
        )

        self._parser.add_argument(
            "--wallet_name",
            type=str,
            help="Wallet name",
            default=self.settings.wallet_name,
        )

        self._parser.add_argument(
            "--hotkey_name",
            type=str,
            help="Hotkey name",
            default=self.settings.hotkey_name,
        )

        self._parser.add_argument(
            "--subtensor.network",
            type=str,
            help="Subtensor network",
            default=self.settings.subtensor.network,
        )

        self._parser.add_argument(
            "--subtensor.address",
            type=str,
            help="Subtensor address",
            default=self.settings.subtensor.address,
        )

        self._parser.add_argument(
            "--mock",
            action="store_true",
            help="Mock neuron and all network components.",
            default=self.settings.mock,
        )

        self._parser.add_argument(
            "--load_old_nodes",
            action="store_true",
            help="Load old nodes",
            default=self.settings.load_old_nodes,
        )

        self._parser.add_argument(
            "--min_stake_threshold",
            type=int,
            help="Minimum stake threshold",
            default=self.settings.min_stake_threshold,
        )

        self._parser.add_argument(
            "--neuron.sync_frequency",
            type=int,
            help="The default sync frequency",
            default=self.settings.neuron.sync_frequency,
        )

        self._parser.add_argument(
            "--neuron.events_retention_size",
            type=str,
            help="Events retention size.",
            default=self.settings.neuron.events_retention_size,
        )

        self._parser.add_argument(
            "--neuron.dont_save_events",
            action="store_true",
            help="If set, we dont save events to a log file.",
            default=self.settings.neuron.dont_save_events,
        )

        self._parser.add_argument(
            "--dht.port",
            type=int,
            help="Port for the DHT.",
            default=self.settings.dht.port,
        )

        self._parser.add_argument(
            "--dht.bootstrap.ip",
            type=str,
            help="Bootstrap node IP for the DHT.",
            default=self.settings.dht.bootstrap.ip,
        )

        self._parser.add_argument(
            "--dht.bootstrap.port",
            type=int,
            help="Bootstrap node port for the DHT.",
            default=self.settings.dht.bootstrap.port,
        )

    def add_miner_args(self):
        self._parser.add_argument(
            "--store_dir",
            type=str,
            help="Directory for the object store",
            default=self.settings.miner.store_dir,
        )

    def add_validator_args(self):
        self._parser.add_argument(
            "--synthetic",
            help="If you want to run a synthetic validator",
            action="store_true",
            default=self.settings.validator.synthetic,
        )

        self._parser.add_argument(
            "--db_dir",
            type=str,
            help="Directory of the validator database",
            default=self.settings.validator.db_dir,
        )

        self._parser.add_argument(
            "--neuron.num_concurrent_forwards",
            type=int,
            help="The number of concurrent forwards running at any time.",
            default=self.settings.validator.neuron.num_concurrent_forwards,
        )

        self._parser.add_argument(
            "--neuron.disable_set_weights",
            action="store_true",
            help="Disables setting weights.",
            default=self.settings.validator.neuron.disable_set_weights,
        )

        self._parser.add_argument(
            "--neuron.moving_average_alpha",
            type=float,
            help="Moving average alpha parameter, how much to add of the new observation.",
            default=self.settings.validator.neuron.moving_average_alpha,
        )

        self._parser.add_argument(
            "--neuron.response_time_alpha",
            type=float,
            help="Moving average alpha parameter for response time scores",
            default=self.settings.validator.neuron.response_time_alpha,
        )

        self._parser.add_argument(
            "--query.batch_size",
            type=int,
            help="max store query batch size",
            default=self.settings.validator.query.batch_size,
        )

        self._parser.add_argument(
            "--query.num_uids",
            type=int,
            help="Number of uids to query per store request",
            default=self.settings.validator.query.num_uids,
        )

        self._parser.add_argument(
            "--query.timeout",
            type=int,
            help="Query timeout",
            default=self.settings.validator.query.timeout,
        )
