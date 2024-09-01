import logging
import logging.config
import os
from datetime import datetime


def setup_logging():
    log_filename = os.path.join("logs", f'{datetime.now().strftime("%Y-%m-%d")}.log')
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.config.dictConfig(
        {
            "version": 1,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                },
            },
            "handlers": {
                "default": {
                    "level": log_level,
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                },
                "logger_file": {
                    "level": log_level,
                    "formatter": "standard",
                    "class": "logging.FileHandler",
                    "filename": log_filename,
                },
            },
            "loggers": {
                "main": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
                "bp": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
                "mongo": {
                    "handlers": ["default", "logger_file"],
                    "level": log_level,
                    "propagate": False,
                },
            },
        }
    )
