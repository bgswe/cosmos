import json
import logging
import warnings
from uuid import UUID, uuid4

from structlog import configure_once, make_filtering_bound_logger
from structlog.processors import JSONRenderer

log_level_map = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}


def configure_logs(log_level: int = logging.ERROR):
    """..."""

    # Raises runing warning one addition config attempts, ignore it
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        configure_once(
            wrapper_class=make_filtering_bound_logger(log_level),
            processors=[JSONRenderer(indent=2, sort_keys=True)],
        )


def get_uuid() -> UUID:
    """Reduces uuid lib uuid4 generation to str type."""

    return uuid4()


def obj_encoder(o):
    if isinstance(o, UUID):
        return str(o)


def json_encode(data):
    return json.dumps(data, default=obj_encoder)
