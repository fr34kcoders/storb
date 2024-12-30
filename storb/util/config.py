"""
Configuration options for Storb

NOTE: When adding new config options, do not add a default to the CLI arguments,
otherwise it will override values in the .env file. Instead, add the default in
the config table classes.
"""

import copy
import os
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Any, Optional

from dotenv import load_dotenv

from storb.constants import (
    MAX_QUERY_BATCH_SIZE,
    NUM_UIDS_QUERY,
    QUERY_TIMEOUT,
    STORE_DIR,
    VALIDATOR_DB_DIR,
)


@dataclass
class ConfigTable:
    netuid: int = 0
    external_ip: str = "0.0.0.0"
    api_port: int = 6969
    post_ip: bool = False

    wallet_name: str = "default"
    hotkey_name: str = "default"

    subtensor_network: str = "finney"
    subtensor_address: str = "wss://entrypoint-finney.opentensor.ai:443"

    mock: bool = False

    neuron_epoch_length: int = 100
    neuron_events_retention_size: int = 2 * 1024 * 1024 * 1024  # 2 GiB
    neuron_dont_save_events: bool = False

    dht_port: int = 6942
    dht_bootstrap_ip: str = None
    dht_bootstrap_port: int = None


@dataclass
class MinerConfigTable(ConfigTable):
    store_dir: str = STORE_DIR


@dataclass
class ValidatorConfigTable(ConfigTable):
    neuron_num_concurrent_forwards: int = 1
    neuron_disable_set_weights: bool = False
    neuron_moving_average_alpha: float = 0.1
    neuron_response_time_alpha: float = 0.1

    synthetic: bool = False
    db_dir: str = VALIDATOR_DB_DIR

    query_batch_size: int = MAX_QUERY_BATCH_SIZE
    num_uids_query: int = NUM_UIDS_QUERY  # TODO: rename to query_num_uids
    query_timeout: int = QUERY_TIMEOUT


ConfigTableT = ConfigTable | MinerConfigTable | ValidatorConfigTable


class Config:
    """Configuration for Storb

    The CLI supports all configurations while the env file supports a subset.
    The configuration option that is stored in `ConfigTable` and passed through via CLI
    arguments is in snake_case. The env file equivalent is the exact same but in
    SCREAMING_SNAKE_CASE.

    As an example, for the configuration option `subtensor_network` in the ConfigTable,
    it will be passed in the CLI as `--subtensor_network` and in the env file, it will
    be `SUBTENSOR_NETWORK`.

    Currently, nested commands and arguments for CLI arguments are not supported.
    """

    def __init__(
        self,
        config_table: ConfigTableT = ConfigTable(),
    ):
        self.T = copy.deepcopy(config_table)
        self._parser = ArgumentParser()
        self.add_args()
        self.save_config()

    def get_config_option(self, key: str, default: Optional[Any] = None) -> Any:
        """Attept to get from config table, then environment variables, then
        the default option, if it exists

        Parameters
        ----------
        key : str
            Config option to get
        default : Any
            An optional default option, if it exists

        Returns
        -------
        The config option
        """

        if hasattr(self.T, key):
            return self.T.__dict__.get(key)
        if os.getenv(key.upper()):
            return os.getenv(key.upper())

        return default

    def save_config(self):
        """Save configurations from the env file and command line arguments.
        Command line arguments take precedence.
        """

        load_dotenv()

        # TODO: Use TOML rather than env
        # for opt in self.T.__dict__.keys():
        #     env_val = os.getenv(opt.upper())
        #     if env_val:
        #         self.T.__dict__[opt] = env_val

        args = self._parser.parse_args()
        for arg, val in vars(args).items():
            if val:
                self.T.__dict__[arg] = val

    def _get_parser_actions(self) -> list[str]:
        """Get command line argument names"""

        actions = []
        for action in self._parser._actions:
            actions.append(action.dest)
        return actions

    def add_args(self):
        self._parser.add_argument(
            "--netuid",
            type=int,
            help="Subnet netuid",
        )

        self._parser.add_argument(
            "--external_ip",
            type=str,
            help="External IP",
        )

        self._parser.add_argument(
            "--api_port",
            type=int,
            help="API port for the node",
        )

        self._parser.add_argument(
            "--post_ip",
            help="If you want to run a organic validator",
            type=lambda x: (str(x).lower() == "true"),
            default=False,
        )

        self._parser.add_argument(
            "--wallet_name",
            type=str,
            help="Wallet name",
        )

        self._parser.add_argument(
            "--hotkey_name",
            type=str,
            help="Hotkey name",
        )

        self._parser.add_argument(
            "--subtensor_network",
            type=str,
            help="Subtensor network",
        )

        self._parser.add_argument(
            "--subtensor_address",
            type=str,
            help="Subtensor address",
        )

        self._parser.add_argument(
            "--mock",
            action="store_true",
            help="Mock neuron and all network components.",
        )

        self._parser.add_argument(
            "--neuron_epoch_length",
            type=int,
            help="The default epoch length (how often we set weights, measured in 12 second blocks).",
        )

        self._parser.add_argument(
            "--neuron_events_retention_size",
            type=str,
            help="Events retention size.",
        )

        self._parser.add_argument(
            "--neuron_dont_save_events",
            action="store_true",
            help="If set, we dont save events to a log file.",
        )

        self._parser.add_argument(
            "--dht_port",
            type=int,
            help="Port for the DHT.",
        )

        self._parser.add_argument(
            "--dht_bootstrap_ip",
            type=str,
            help="Bootstrap node IP for the DHT.",
        )

        self._parser.add_argument(
            "--dht_bootstrap_port",
            type=int,
            help="Bootstrap node port for the DHT.",
        )


class MinerConfig(Config):
    def __init__(
        self,
        config_table: MinerConfigTable = MinerConfigTable(),
    ):
        super().__init__(config_table)
        # self.save_config()


def add_miner_args(cls, parser):
    parser.add_argument(
        "--store_dir",
        type=str,
        help="Directory for the object store",
    )


class ValidatorConfig(Config):
    def __init__(
        self,
        config_table: ValidatorConfigTable = ValidatorConfigTable(),
    ):
        super().__init__(config_table)
        print("does this shit get called")
        # self.save_config()


def add_validator_args(cls, parser):
    parser.add_argument(
        "--neuron_num_concurrent_forwards",
        type=int,
        help="The number of concurrent forwards running at any time.",
    )

    parser.add_argument(
        "--neuron_disable_set_weights",
        action="store_true",
        help="Disables setting weights.",
    )

    parser.add_argument(
        "--neuron_moving_average_alpha",
        type=float,
        help="Moving average alpha parameter, how much to add of the new observation.",
    )

    parser.add_argument(
        "--neuron_response_time_alpha",
        type=float,
        help="Moving average alpha parameter for response time scores",
    )

    parser.add_argument(
        "--synthetic",
        help="If you want to run a synthetic validator",
        action="store_true",
    )

    parser.add_argument(
        "--db_dir",
        type=str,
        help="Directory of the validator database",
    )

    parser.add_argument(
        "--query_batch_size",
        type=int,
        help="max store query batch size",
    )

    parser.add_argument(
        "--num_uids_query",
        type=int,
        help="Number of uids to query per store request",
    )

    parser.add_argument(
        "--query_timeout",
        type=int,
        help="Query timeout",
    )
