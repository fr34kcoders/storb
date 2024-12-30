import random

import numpy as np
from fiber.chain.metagraph import Metagraph

from storb.neuron import Neuron


def check_hotkey_availability(metagraph: Metagraph, hotkey: str) -> bool:
    """
    Check if uid is available. The UID should be available if it exists and
    does not have vtrust set (i.e. they're a miner).

    Parameters
    ----------
    metagraph : Metagraph
        Metagraph object
    uid : int
        uid to be checked

    Returns
    -------
    bool
        True if uid is available, False otherwise
    """

    node = metagraph.nodes.get(hotkey)
    if not node:
        return False

    # If the neuron is a validator then it shouldn't have vtrust set
    if node.vtrust != 0:
        return False

    return True


def get_random_hotkeys(self: Neuron, k: int, exclude: list[int] = None) -> list[str]:
    """
    Returns k available random uids from the metagraph.

    Parameters
    ----------
    k : int
        Number of uids to return.
    exclude : list[int]
        List of uids to exclude from the random sampling.

    Returns
    -------
    uids : np.ndarray
        Randomly sampled available uids.

    Notes
    -----
    If `k` is larger than the number of available `uids`, set `k` to
    the number of available `uids`.
    """

    candidate_hotkeys = []
    avail_hotkeys = []

    for hotkey in self.metagraph.nodes:
        hotkey_is_available = check_hotkey_availability(self.metagraph, hotkey)
        hotkey_is_not_excluded = exclude is None or hotkey not in exclude

        if hotkey_is_available:
            avail_hotkeys.append(hotkey)
            if hotkey_is_not_excluded:
                candidate_hotkeys.append(hotkey)

    # If k is larger than the number of available hotkeys, set k to the number of available hotkeys.
    k = min(k, len(avail_hotkeys))
    # Check if candidate_hotkeys contain enough for querying, if not grab all avaliable hotkeys
    available_hotkeys = candidate_hotkeys

    if len(candidate_hotkeys) < k:
        available_hotkeys += random.sample(
            [hotkey for hotkey in avail_hotkeys if hotkey not in candidate_hotkeys],
            k - len(candidate_hotkeys),
        )
    hotkeys = random.sample(available_hotkeys, k)
    return hotkeys
