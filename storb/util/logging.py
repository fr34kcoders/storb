import logging
import os
from logging.handlers import RotatingFileHandler

from colorlog import ColoredFormatter

# Define custom log levels
EVENTS_LEVEL_NUM = 38
DEFAULT_LOG_BACKUP_COUNT = 10
LOG_DIR = "logs"


# Function to ensure the log directory exists
def ensure_log_directory_exists(log_dir: str):
    """Ensure the log directory exists. Create it if it doesn't.

    Parameters
    ----------
    log_dir : str
        Path to the log directory.
    """

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)


def setup_rotating_logger(
    logger_name: str, log_level: int, max_size: int, log_dir: str = LOG_DIR
) -> logging.Logger:
    """Set up a logger with rotating file and stream handlers.

    Parameters
    ----------
    logger_name : str
        Name of the logger.
    log_level : int
        Logging level.
    max_size : int
        Maximum size of each log file before rotation (in bytes).
    log_dir : str
        Directory for the log files.

    Returns
    -------
    logging.Logger
        Configured logger.
    """

    ensure_log_directory_exists(log_dir)

    # Define log file
    log_file = os.path.join(log_dir, f"{logger_name}.log")

    # Create logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)

    # Define log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    color_formatter = ColoredFormatter(
        "%(log_color)s%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        log_colors={
            "DEBUG": "cyan",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
            "EVENT": "blue",
        },
    )

    # Rotating file handler
    file_handler = RotatingFileHandler(
        log_file, maxBytes=max_size, backupCount=DEFAULT_LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)
    logger.addHandler(file_handler)

    # Stream handler for console output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)

    return logger


def setup_event_logger(retention_size: int, logs_dir: str = LOG_DIR) -> logging.Logger:
    """Set up a logger for event-level logs with a custom log level.

    Parameters
    ----------
    logs_dir : str
        Directory path for log files.
    retention_size : int
        Maximum size of the event log file before rotation.

    Returns
    -------
    logging.Logger
        Configured event logger.
    """

    ensure_log_directory_exists(logs_dir)

    # Register custom EVENT log level
    logging.addLevelName(EVENTS_LEVEL_NUM, "EVENT")

    def event(self, message, *args, **kws):
        if self.isEnabledFor(EVENTS_LEVEL_NUM):
            self._log(EVENTS_LEVEL_NUM, message, args, **kws)

    logging.Logger.event = event

    # Configure event logger
    return setup_rotating_logger("event", EVENTS_LEVEL_NUM, retention_size, logs_dir)


UVICORN_LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "access": {
            "format": "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "default": {
            "level": "INFO",
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
        "access": {
            "level": "INFO",
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["access"],
            "level": "INFO",
            "propagate": False,
        },
        "fastapi_logger": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}
