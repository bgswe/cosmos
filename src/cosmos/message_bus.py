from typing import Protocol
from uuid import UUID

from structlog import get_logger

from cosmos.domain import (
    Command,
    Event,
)
from cosmos.unit_of_work import UnitOfWork

logger = get_logger()


class EventHandler(Protocol):
    """Callback Protocol for an EventHandler function."""

    async def __call__(self, uow: UnitOfWork, event: Event):
        ...


class CommandHandler(Protocol):
    """Callback Protocol for a CommandHandler function."""

    async def __call__(self, uow: UnitOfWork, command: Command):
        ...
