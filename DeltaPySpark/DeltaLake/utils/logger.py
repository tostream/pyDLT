import logging
import os
import sys


def initialize_logger(logger_name: str,
                      log_file: str = None,
                      log_level: int = logging.DEBUG,
                      log_rotate_max_size: int = 1024 * 1000 * 50,
                      log_rotate_count: int = 5) -> logging.Logger:
    """_summary_

    Args:
        logger_name (str): _description_
        log_file (str, optional): _description_. Defaults to None.
        log_level (int, optional): _description_. Defaults to logging.DEBUG.
        log_rotate_max_size (int, optional): _description_. Defaults to 1024*1000*50.
        log_rotate_count (int, optional): _description_. Defaults to 5.

    Returns:
        logging.Logger: _description_
    """
    
    formatter = logging.Formatter(
        '[%(name)-10s][%(levelname)-5s] %(asctime)s - %(message)s')
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger.setLevel(log_level)
    return logger
