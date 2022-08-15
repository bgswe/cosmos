import uuid

from structlog import configure
from structlog import get_logger as get_structlog_logger
from structlog.processors import JSONRenderer


def uuid4() -> str:
    """Reduces uuid lib uuid4 generation to str type."""

    return str(uuid.uuid4())


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


# EventCollector = Callable[[Repository], Iterator[Event]]

# def simple_collector(repository: Repository) -> Iterator[Event]:
#     for aggregate in repository.seen:
#         while aggregate.has_events:
#             # I believe we yield to allow the handling of this event
#             # to generate events itself
#             yield aggregate.get_events().pop(0)
