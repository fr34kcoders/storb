import base64
import getpass
import json
from pathlib import Path

from cryptography.hazmat.primitives import serialization

from storb.challenge import APDPKey
from storb.constants import DEFAULT_RSA_KEY_SIZE
from storb.util.logging import get_logger

logger = get_logger(__name__)


class KeyManager:
    def __init__(self, key: APDPKey, pem_file: Path = None):
        """
        Key Manager for handling RSA keys and metadata.

        Parameters
        ----------
        key : APDPKey
            APDPKey instance to manage.
        pem_file : Path, optional
            Path to the PEM file to save the keys, by default None
        """

        if not pem_file:
            raise ValueError("PEM file path is required.")
        self.pem_file = Path(pem_file)
        self.key = key

        self.initialize_keys()

    def prompt_confirmation(self, message: str) -> bool:
        """
        Prompt the user for a yes/no confirmation.

        Parameters
        ----------
        message : str
            Message to display to the user.

        Returns
        -------
        bool
            True if user responds with 'yes', False otherwise.
        """

        while True:
            response = input(f"{message} (yes/no): ").strip().lower()
            if response in ["yes", "no"]:
                return response == "yes"
            logger.warning("Please respond with 'yes' or 'no'.")

    def initialize_keys(self, rsa_bits=DEFAULT_RSA_KEY_SIZE):
        """
        Initialize the keys. If the PEM file exists, confirm with the user to either
        load the existing key or regenerate a new one.

        Parameters
        ----------
        rsa_bits : int, optional
            Number of bits for the RSA key, by default 2048
        """

        if self.pem_file.exists():
            logger.info(f"Key file {self.pem_file} found.")
            if self.prompt_confirmation("Do you want to use the existing key?"):
                try:
                    password = getpass.getpass(
                        "Enter the password to unlock the keys: "
                    )
                    self.load_keys(password)
                    logger.info("Keys successfully loaded.")
                    return
                except Exception as e:
                    logger.error(f"Failed to load keys: {e}")
                    return

            if not self.prompt_confirmation(
                "Regenerating keys will invalidate previously signed pieces. Do you want to proceed?"
            ):
                logger.info("Key initialization canceled by the user.")
                return

        if not self.prompt_confirmation(
            "No key file found. Do you want to generate new keys?"
        ):
            logger.info("Key initialization canceled by the user.")
            return

        logger.info("Generating new keys...")
        self.key.generate(rsa_bits)
        self.save_keys_with_password()

    def save_keys_with_password(self):
        """
        Save keys after confirming the password twice.
        """

        while True:
            password = getpass.getpass("Enter a password to encrypt the keys: ")
            confirm_password = getpass.getpass("Confirm the password: ")
            if password == confirm_password:
                break

            logger.warning("Passwords do not match. Please try again.")

        self.serialize_keys(password)

    def serialize_keys(self, password: str):
        """
        Serialize the RSA key to PEM format and append metadata as JSON.

        Parameters
        ----------
        password : str
            Password to encrypt the RSA key:
        """

        try:
            pem_data = self.key.rsa.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.BestAvailableEncryption(
                    password.encode()
                ),
            ).decode()

            # Serialize metadata
            metadata = {
                "g": self.key.g,
                "prf_key": base64.b64encode(self.key.prf_key).decode()
                if self.key.prf_key
                else None,
            }
            metadata_json = json.dumps(metadata)

            with open(self.pem_file, "w") as f:
                f.write(pem_data)
                f.write("\n-----BEGIN METADATA-----\n")
                f.write(metadata_json)
                f.write("\n-----END METADATA-----\n")

            logger.info(f"Keys saved to {self.pem_file}.")
        except Exception as e:
            logger.error(f"Failed to save keys: {e}")
            raise

    def load_keys(self, password: str):
        """
        Load the RSA key and metadata from the PEM file.

        Parameters
        ----------
        password : str
            Password to decrypt the RSA key.
        """

        try:
            with open(self.pem_file, "r") as f:
                lines = f.readlines()

            # Separate PEM and metadata
            pem_data = []
            metadata_lines = []
            in_metadata = False
            for line in lines:
                if line.strip() == "-----BEGIN METADATA-----":
                    in_metadata = True
                    continue
                elif line.strip() == "-----END METADATA-----":
                    in_metadata = False
                    continue

                if in_metadata:
                    metadata_lines.append(line.strip())
                else:
                    pem_data.append(line)

            pem_data = "".join(pem_data)
            metadata = json.loads("".join(metadata_lines))

            # Load RSA key
            self.key.rsa = serialization.load_pem_private_key(
                pem_data.encode(),
                password=password.encode(),
            )

            # Load metadata
            self.key.g = metadata["g"]
            self.key.prf_key = (
                base64.b64decode(metadata["prf_key"]) if metadata["prf_key"] else None
            )

            logger.info(f"Keys successfully loaded from {self.pem_file}.")
        except Exception as e:
            logger.error(f"Failed to load keys: {e}")
            raise
