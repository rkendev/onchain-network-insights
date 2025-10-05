"""
common.logging_setup

Set up standard logging for the project.
"""
import logging

def setup_logging(level: int = logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
