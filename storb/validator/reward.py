import numpy as np


def get_response_rate_scores(
    self,
    miner_stats: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Returns an array of scores for the given miner stats

    Parameters
    ----------
    miner_stats : dict
        A dictionary of miner statistics

    Returns
    -------
    np.ndarray
        Arrays of uids and rewards for the given query and responses.
    """

    uids = []
    weighted_rate_sums = []

    uids_filter = list(range(len(self.metagraph.nodes)))
    for uid, miner_stats in miner_stats.items():
        if uid not in uids_filter:
            continue
        uids.append(uid)
        ret_attempts = max(miner_stats.get("retrieval_attempts", 1), 1)
        store_attempts = max(miner_stats.get("store_attempts", 1), 1)
        retrieval_rate = abs(miner_stats.get("retrieval_successes", 0) / ret_attempts)
        store_rate = abs(miner_stats.get("store_successes", 0) / store_attempts)
        weighted_rate_sum = (retrieval_rate / 2) + (store_rate / 2)
        weighted_rate_sums.append(weighted_rate_sum)

    uids = np.array(uids)
    sum_max = max(*weighted_rate_sums)
    sum_max = 1 if sum_max == 0 else sum_max
    scores = np.asarray(weighted_rate_sums) / sum_max

    return uids, scores


def get_challenge_scores(
    self,
    miner_stats: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """Returns an array of scores for the given miner stats

    Parameters
    ----------
    miner_stats : dict
        A dictionary of miner statistics

    Returns
    -------
    np.ndarray
        An array of rewards for the given query and responses.
    """

    uids = []
    success_rates = []

    uids_filter = list(range(len(self.metagraph.nodes)))
    for uid, miner_stats in miner_stats.items():
        if uid not in uids_filter:
            continue
        uids.append(uid)
        challenge_attempts = max(miner_stats.get("challenge_attempts", 1), 1)
        success_rate = abs(
            miner_stats.get("challenge_successes", 0) / challenge_attempts
        )
        success_rates.append(success_rate)

    uids = np.array(uids)
    scores = np.asarray(success_rates) / max(*success_rates)

    return uids, scores
