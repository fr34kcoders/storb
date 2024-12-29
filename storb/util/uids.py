import random

import numpy as np
from fiber.chain.metagraph import Metagraph

from storb.neuron import Neuron


def check_uid_availability(metagraph: Metagraph, uid: int) -> bool:
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

    node = metagraph.nodes.get(uid)
    if not node:
        return False

    # If the neuron is a validator then it shouldn't have vtrust set
    if not node.vtrust or node.vtrust == 0:
        return False

    return True


def get_random_uids(self: Neuron, k: int, exclude: list[int] = None) -> np.ndarray:
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

    candidate_uids = []
    avail_uids = []

    for uid in range(len(self.metagraph.nodes)):
        uid_is_available = check_uid_availability(self.metagraph, uid)
        uid_is_not_excluded = exclude is None or uid not in exclude

        if uid_is_available:
            avail_uids.append(uid)
            if uid_is_not_excluded:
                candidate_uids.append(uid)

    # If k is larger than the number of available uids, set k to the number of available uids.
    k = min(k, len(avail_uids))
    # Check if candidate_uids contain enough for querying, if not grab all avaliable uids
    available_uids = candidate_uids

    if len(candidate_uids) < k:
        available_uids += random.sample(
            [uid for uid in avail_uids if uid not in candidate_uids],
            k - len(candidate_uids),
        )
    uids = np.array(random.sample(available_uids, k))
    return uids
