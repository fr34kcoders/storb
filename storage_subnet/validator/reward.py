# The MIT License (MIT)
# Copyright (c) 2023 Yuma Rao
# TODO(developer): Set your name
# Copyright (c) 2023 <your name>

# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
# documentation files (the “Software”), to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all copies or substantial portions of
# the Software.

# THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
# THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.
import numpy as np


def get_response_rate_scores(
    self,
    miner_stats: dict,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Returns an array of scores for the given miner stats

    Args:
    - miner_stats (dicy): A dictionary of miner statistics

    Returns:
    - np.ndarray: An array of rewards for the given query and responses.
    """

    uids = []
    weighted_rate_sums = []

    uids_filter = list(range(self.metagraph.n))
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
