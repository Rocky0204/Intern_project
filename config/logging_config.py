import logging.config
import os

DEFAULT_CONSOLE_LOG_LEVEL = os.getenv("LOG_LEVEL_CONSOLE", "INFO").upper()

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(asctime)s - %(name)s - %(levelname)s - "%(request_line)s"'
            + " %(status_code)s",
            "datefmt": "%Y-%m-%dT%H:%M:%S%z",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "standard",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "root": {"handlers": ["console"], "level": DEFAULT_CONSOLE_LOG_LEVEL},
        "uvicorn.error": {"handlers": ["console"], "propagate": False},
        "uvicorn.access": {"handlers": ["access"], "propagate": False},
    },
}


def setup_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
