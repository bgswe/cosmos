from datetime import datetime as dt
from typing import Protocol

from dependency_injector.wiring import Provide

from cosmos.unit_of_work import UnitOfWork
from cosmos.domain import Command, CommandComplete, CommandCompletionStatus, Event


class CommandHandler(Protocol):
    """Callback Protocol for a CommandHandler function."""

    async def __call__(self, uow: UnitOfWork, command: Command):
        ...


def command(handler_func: CommandHandler):
    """Decorator used atop command handler functions

    This provides wiring of the UnitOfWork dependency, and allows complete
    decoupling of command handlers and the UnitOfWork implementation.
    """

    async def inner_func(
        *, uow: UnitOfWork = Provide["unit_of_work"], command: Command
    ):
        # enable ability to run ancillary code after command handled, under single transaction
        async with uow as uow:
            # invoke decorated business logic with given command
            await handler_func(uow=uow, command=command)

        completion_event = CommandComplete(
            command_id=command.message_id,
            command_name=command.name,
            timestamp=dt.now(),
            status=CommandCompletionStatus.SUCCESS,
        )

        async with uow as uow:
            await uow.outbox.send(messages=[completion_event])

    return inner_func


class EventHandler(Protocol):
    """Callback Protocol for an EventHandler function."""

    async def __call__(self, uow: UnitOfWork, event: Event):
        ...
