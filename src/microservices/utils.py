from uuid import UUID, uuid4

from structlog import configure
from structlog import get_logger as get_structlog_logger
from structlog.processors import JSONRenderer


def get_uuid() -> UUID:
    """Reduces uuid lib uuid4 generation to str type."""

    return uuid4()


configured = False


def get_logger():
    """Provides a structlog logger for use anywhere.

    EVAL: It's undecided whether any logger abstraction is necessary
    here. We could define a logger interface and then provide wrapper
    a wrapper class for the current implementation.
    """
    if not configured:
        configure(processors=[JSONRenderer(indent=2, sort_keys=True)])

    return get_structlog_logger()
