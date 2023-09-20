from typing import Protocol, Type

from dependency_injector.wiring import Provide

from cosmos.unit_of_work import UnitOfWork
from cosmos.domain import Command, Message


class MessageHandler(Protocol):
    """Callback Protocol for a CommandHandler function."""

    async def __call__(self, message: Message, uow: UnitOfWork):
        ...


class RegistersMessage(Protocol):
    """Structural subtype for the an object whom registers a message w/ its handler"""

    def register_message(self, message_type: Type[Message], handler):
        ...


def command(handler_func: MessageHandler):
    """Decorator used atop command handler functions

    This provides wiring of the UnitOfWork dependency, and allows complete
    decoupling of command handlers and the UnitOfWork implementation.
    """

    async def inner_func(
        *,
        uow: UnitOfWork = Provide["unit_of_work"],
        command: Command,
    ):
        # enable ability to run ancillary code after command handled, under single transaction
        async with uow as uow:
            # ensure idempotency by checking if messages has been processed
            processed_record = await uow.processed_messages.is_processed(
                message_id=command.message_id
            )

            if processed_record:
                return

            # invoke decorated business logic with given command
            await handler_func(uow=uow, command=command)

            # to ensure message idempotency we record message ID as processed
            await uow.processed_messages.mark_processed(message_id=command.message_id)

    return inner_func
