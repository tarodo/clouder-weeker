import logging
import logging.config
import os
from datetime import datetime

def setup_logging():
    log_filename = os.path.join('logs', f'{datetime.now().strftime("%Y-%m-%d")}.log')

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
                    "level": "DEBUG",
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                },
                "logger_file": {
                    "level": "DEBUG",
                    "formatter": "standard",
                    "class": "logging.FileHandler",
                    "filename": log_filename,
                },
            },
            "loggers": {
                "main": {"handlers": ["default"], "level": "DEBUG", "propagate": False},
                "clouder": {"handlers": ["default", "logger_file"], "level": "DEBUG", "propagate": False},
            },
        }
    )

setup_logging()

bp_logger = logging.getLogger('bp')

bp_logger.debug('This is a debug message')
bp_logger.info('This is an info message')
bp_logger.warning('This is a warning message')
bp_logger.error('This is an error message')