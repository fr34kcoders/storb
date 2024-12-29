"""Storb: An object storage subnet on the Bittensor network"""

__version__ = "0.1.0"

_version_split = __version__.split(".")
__spec_version__ = (
    (1000 * int(_version_split[0]))
    + (10 * int(_version_split[1]))
    + (1 * int(_version_split[2]))
)
