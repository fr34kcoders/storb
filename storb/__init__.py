"""Storb: An object storage subnet on the Bittensor network"""

__version__ = "0.2.2"


def get_spec_version(version: str) -> int:
    version_split = version.split(".")
    return (
        (1000 * int(version_split[0]))
        + (10 * int(version_split[1]))
        + (1 * int(version_split[2]))
    )


__spec_version__ = get_spec_version(__version__)
