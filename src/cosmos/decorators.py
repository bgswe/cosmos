from datetime import datetime as dt
from typing import Callable, Protocol, Type
from uuid import UUID

from dependency_injector.wiring import Provide

from cosmos.unit_of_work import UnitOfWork
from cosmos.domain import (
    Message,
)


class MessageHandler(Protocol):
    """Callback Protocol for a CommandHandler function."""

    async def __call__(self, message: Message, uow: UnitOfWork):
        ...


class RegistersMessage(Protocol):
    """Structural subtype for the an object whom registers a message w/ its handler"""

    def register_message(self, message_type: Type[Message], handler):
        ...


def command(
    *, message_registrar: Provide["message_registrar"], message_type: Type[Message]
):
    def decorator(handler_func: MessageHandler):
        """Decorator used atop command handler functions

        This provides wiring of the UnitOfWork dependency, and allows complete
        decoupling of command handlers and the UnitOfWork implementation.
        """

        async def inner_func(
            message: Message,
            uow: UnitOfWork = Provide["unit_of_work"],
        ):
            # enable ability to run ancillary code after command handled, under single transaction
            async with uow as uow:
                # ensure idempotency by checking if messages has been processed
                if uow.processed_messages.is_processed(id=message.id):
                    return

                # invoke decorated business logic with given command
                await handler_func(message, uow)

                # to ensure message idempotency we record message ID as processed
                uow.processed_messages.mark_processed(id=message.id)

        message_registrar.register_message(
            message_type=message_type,
            handler=inner_func,
        )

        return inner_func
