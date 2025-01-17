import hashlib
import json


def generate_infohash(
    filename: str, timestamp: str, piece_length: int, length: int, pieces: list[str]
) -> tuple[str, dict[str, str | int | list[str]]]:
    """Generate the infohash from metadata and piece hashes.

    Parameters
    ----------
    name : str
        Name of the file.
    timestamp : str
        Timestamp of file creation/upload.
    piece_length : int
        Length of each piece in bytes.
    length : int
        Total length of the file in bytes.
    pieces : list[str]
        List of SHA-1 hashes of the pieces.

    Returns
    -------
    Tuple[str, Dict[str, str | int | List[str]]]:
        - str: SHA-256 hash of the serialized metadata (infohash).
        - Dict: The serialized infohash dictionary.
    """

    # Create the infohash metadata dictionary
    infohash_data: dict[str, str | int | list[str]] = {
        "filename": filename,
        "timestamp": timestamp,
        "piece_length": piece_length,
        "length": length,
        "pieces": pieces,
    }

    # Serialize the dictionary into JSON and calculate its SHA-1 hash
    infohash_json = json.dumps(infohash_data, separators=(",", ":")).encode("utf-8")
    infohash = hashlib.sha256(infohash_json).hexdigest()

    return infohash, infohash_data
