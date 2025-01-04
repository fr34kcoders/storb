import logging
import os
import sys
from enum import StrEnum
from typing import List, Optional

from colorama import Back, Fore, Style, init

# Initialize colorama for cross-platform color support
init(autoreset=True)


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @property
    def level(self) -> int:
        """Maps the LogLevel enum to the corresponding logging module constant.

        Returns
        -------
        int
            The logging module constant for the log level.
        """
        return {
            LogLevel.DEBUG: logging.DEBUG,
            LogLevel.INFO: logging.INFO,
            LogLevel.WARNING: logging.WARNING,
            LogLevel.ERROR: logging.ERROR,
            LogLevel.CRITICAL: logging.CRITICAL,
        }[self]


class ColoredFormatter(logging.Formatter):
    """Custom logging formatter to add colors based on log level."""

    LEVEL_COLORS = {
        LogLevel.DEBUG: Fore.CYAN,
        LogLevel.INFO: Fore.GREEN,
        LogLevel.WARNING: Fore.YELLOW,
        LogLevel.ERROR: Fore.RED,
        LogLevel.CRITICAL: Fore.RED + Back.WHITE,
    }

    def format(self, record: logging.LogRecord) -> str:
        try:
            log_level = LogLevel(record.levelname)
            color = self.LEVEL_COLORS.get(log_level, Fore.WHITE)
        except ValueError:
            color = Fore.WHITE  # Default color for unknown levels

        # Apply color to the level name
        levelname_color = f"{color}{Style.BRIGHT}{record.levelname}{Style.RESET_ALL}"

        # Create a copy of the record to avoid modifying the original
        record_copy = logging.LogRecord(
            name=record.name,
            level=record.levelno,
            pathname=record.pathname,
            lineno=record.lineno,
            msg=record.msg,
            args=record.args,
            exc_info=record.exc_info,
            func=record.funcName,
            sinfo=record.stack_info,
        )
        record_copy.levelname = levelname_color

        # Format the message using the parent class
        formatted_message = super().format(record_copy)

        return formatted_message


def get_logger(
    name: str,
    log_format: Optional[str] = None,
    date_format: Optional[str] = None,
    handlers: Optional[List[logging.Handler]] = None,
) -> logging.Logger:
    """Initializes and returns a logger with the specified configurations,
    reading the log level from the environment variable LOG_LEVEL.

    Parameters
    ----------
    name: str
        The name of the logger.
    log_format: Optional[str]
        Custom format string for log messages. If not provided, a default format is used.
    date_format: Optional[str]
        Custom date format string. If not provided, defaults to "%Y-%m-%d %H:%M:%S".
    handlers: Optional[List[logging.Handler]]
        A list of logging handlers to attach to the logger. If not provided, a StreamHandler is used.

    Returns
    -------
    logging.Logger
        Configured logger instance.
    """
    # Retrieve log level from environment variable, default to INFO
    env_log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    try:
        log_level = LogLevel(env_log_level)
    except ValueError:
        # Initialize a temporary logger to log the warning before main logger is set
        temp_logger = logging.getLogger("temp_logger")
        temp_logger.setLevel(logging.INFO)
        temp_handler = logging.StreamHandler(sys.stdout)
        temp_formatter = logging.Formatter(
            f"{Fore.YELLOW}WARNING{Style.RESET_ALL}: "
            f"Invalid LOG_LEVEL '{env_log_level}' specified. Defaulting to INFO level."
        )
        temp_handler.setFormatter(temp_formatter)
        temp_logger.addHandler(temp_handler)
        temp_logger.warning(
            f"Invalid LOG_LEVEL '{env_log_level}' specified. Defaulting to INFO level."
        )
        log_level = LogLevel.INFO

    logger = logging.getLogger(name)
    logger.setLevel(log_level.level)

    # Clear existing handlers to prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # Define default log format if not provided
    if not log_format:
        log_format = (
            f"{Fore.BLUE}%(asctime)s.%(msecs)03d{Style.RESET_ALL} | "
            f"%(levelname)s | "
            f"{Fore.BLUE}%(name)s{Style.RESET_ALL}:"
            f"{Fore.BLUE}%(funcName)s{Style.RESET_ALL}:"
            f"{Fore.BLUE}%(lineno)d{Style.RESET_ALL} - "
            f"%(message)s"
        )

    # Define default date format if not provided
    if not date_format:
        date_format = "%Y-%m-%d %H:%M:%S"

    # Initialize the formatter
    formatter = ColoredFormatter(log_format, datefmt=date_format)

    # If no handlers are provided, use StreamHandler
    if not handlers:
        handlers = [logging.StreamHandler(sys.stdout)]

    # Attach handlers with the formatter
    for handler in handlers:
        handler.setLevel(log_level.level)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.debug(f"Logger '{name}' initialized with level {log_level}")
    return logger
