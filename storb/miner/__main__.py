import asyncio
import time

from storb.util.logging import get_logger

from storb.miner import Miner

logger = get_logger(__name__)


async def main():
    miner = Miner()
    await miner.start()

    logger.info(f"Miner running... timestamp: {time.time()}")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        await miner.stop()


if __name__ == "__main__":
    asyncio.run(main())
