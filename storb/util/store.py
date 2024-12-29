"""
Provides utilities for accessing and managing the object store
"""

from pathlib import Path

import aiofiles
from fiber.logging_utils import get_logger

logger = get_logger(__name__)


class ObjectStore:
    def __init__(self, store_dir: str):
        """Initialise the object store.

        Parameters
        ----------
        store_dir : int
            The (relative) directory of the object store.
        """

        self.path = Path(store_dir)

        if self.path.exists():
            return

        self.path.mkdir(parents=True)
        for i in range(0xFF + 1):
            Path(self.path / f"{i:02x}").mkdir()

    async def read(self, piece_hash: str) -> bytes:
        """Read piece data in bytes from the store.

        Parameters
        ----------
        piece_hash : str
            The piece hash to query the data by.

        Returns
        -------
        bytes
            The piece data.
        """

        path = self.path / piece_hash[0:2] / piece_hash[2:]

        async with aiofiles.open(path, mode="rb") as f:
            contents = await f.read()
            return contents

    async def write(self, piece_hash: str, data: bytes):
        """Write piece data in bytes to the store.

        Parameters
        ----------
        piece_hash : str
            Piece hash for the data.
        data : bytes
            The piece data in bytes.
        """

        logger.debug(f"Writing piece {piece_hash} to store")
        folder = self.path / piece_hash[0:2]
        if not folder.exists():
            folder.mkdir()

        path = folder / piece_hash[2:]

        async with aiofiles.open(path, mode="wb") as f:
            await f.write(data)
