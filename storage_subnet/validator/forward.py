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

import asyncio

import bittensor as bt

import storage_subnet.validator.db as db
from storage_subnet.validator.reward import get_response_rate_scores


async def forward(self):
    """
    The forward function is called by the validator every time step.

    It is responsible for querying the network and scoring the responses.

    Args:
        self (:obj:`bittensor.neuron.Neuron`): The neuron object which contains all the necessary state for the validator.

    """
    # TODO: challenge miners - based on: https://dl.acm.org/doi/10.1145/1315245.1315318

    # TODO: should we lock the db when scoring?
    # scoring

    # obtain all miner stats from the validator database
    async with db.get_db_connection(self.config.db_dir) as conn:
        miner_stats = await db.get_all_miner_stats(conn)

    # calculate the score(s) for uids given their stats
    response_rate_uids, response_rate_scores = get_response_rate_scores(
        self, miner_stats
    )
    bt.logging.debug(f"response rate scores: {response_rate_scores}")
    bt.logging.debug(f"moving avg. latencies: {self.latencies}")
    bt.logging.debug(f"moving avg. latency scores: {self.latency_scores}")

    # TODO: this should also take the "pdp challenge score" into account
    # TODO: this is a little cooked ngl - will see if it is OK to go without indexing
    rewards = (
        0.2 * self.latency_scores[: len(response_rate_uids)]
        + 0.3 * response_rate_scores[: len(response_rate_uids)]
    )
    self.update_scores(self, rewards, response_rate_uids)

    await asyncio.sleep(5)


async def query_miner(
    self,
    synapse: bt.Synapse,
    uid: str,
    deserialize: bool = False,
) -> tuple[int, bt.Synapse]:
    return uid, await self.dendrite.forward(
        axons=self.metagraph.axons[int(uid)],
        synapse=synapse,
        timeout=self.config.query_timeout,
        deserialize=deserialize,
        streaming=False,
    )


async def query_multiple_miners(
    self,
    synapse: bt.Synapse,
    uids: list[str],
    deserialize: bool = False,
) -> list[bt.Synapse]:
    uid_to_query_task = {
        uid: asyncio.create_task(query_miner(self, synapse, uid, deserialize))
        for uid in uids
    }
    return await asyncio.gather(*uid_to_query_task.values())
