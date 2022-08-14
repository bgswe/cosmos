import uuid

from structlog import configure_once
from structlog import get_logger as get_structlog_logger
from structlog.processors import JSONRenderer


def uuid4() -> str:
    """Reduces uuid lib uuid4 generation to str type."""

    return str(uuid.uuid4())


def get_logger():
    """Provides a structlog logger for use anywhere.

    EVAL: It's undecided whether any logger abstraction is necessary
    here. We could define a logger interface and then provide wrapper
    a wrapper class for the current implementation.
    """

    configure_once(processors=[JSONRenderer(indent=2, sort_keys=True)])

    return get_structlog_logger()
