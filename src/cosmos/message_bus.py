import asyncio
from datetime import datetime as dt
from typing import Dict, List, Protocol, Type
from uuid import UUID

from structlog import get_logger, tracebacks

from cosmos.domain import (
    Command,
    CommandComplete,
    CommandCompletionStatus,
    Event,
    EventPublish,
    Message,
)
from cosmos.unit_of_work import AsyncUnitOfWork, AsyncUnitOfWorkFactory

logger = get_logger()


class EventHandler(Protocol):
    """Callback Protocol for an EventHandler function."""

    async def __call__(self, uow: AsyncUnitOfWork, event: Event):
        ...


class CommandHandler(Protocol):
    """Callback Protocol for a CommandHandler function."""

    async def __call__(self, uow: AsyncUnitOfWork, command: Command):
        ...


class MessageBus:
    """Core engine that is synchronously driven by domain commands.

    The MessageBus waits for messages sent to it through its handle
    method, which can be either a Command, or Event. Commands are
    client driven actions, while events originate from the system.
    Handlers for each are given at initialization in addition to
    a label for the domain, a UnitOfWork factory, and an optional
    external publisher.
    """

    def __init__(
        self,
        uow_factory: AsyncUnitOfWorkFactory,
        event_handlers: Dict[str, List[EventHandler]] | None = None,
        command_handlers: Dict[Type[Command], CommandHandler] | None = None,
    ):
        if event_handlers is None:
            event_handlers = {}

        if command_handlers is None:
            command_handlers = {}

        self._uow_factory = uow_factory

        # TODO: Validate handlers
        self._command_handlers = command_handlers  # should be command.name:Handler
        self._event_handlers = event_handlers

    async def handle(self, message: Message):
        """The external interface to send a message through the system.

        This method takes a command or event and invokes the configured handlers
        passed in upon initialization.

        :param: message -> Command or Event entering the system
        """

        log = logger.bind(message=message)

        logger.debug("top of bus handle")

        # invoke proper handle method based on message type
        if isinstance(message, Event):
            await self._handle_event(event=message)
        elif isinstance(message, Command):
            await self._handle_command(command=message)
        else:
            err_message = "message is not of type Event, nor Command"

            log = logger.bind(err_message=err_message, type=type(message))
            log.error(err_message)

            raise Exception(f"{err_message}, type -> {type(message)}")

        log.debug("bottom of bus handle")

    def handle_no_await(self, message: Message):
        """Provides interface to invoke async handle w/o awaiting."""

        loop = asyncio.get_running_loop()
        loop.create_task(self.handle(message=message))

    async def _handle_event(self, event: Event):
        """Coordinates lifecycle of event handling.

        This method is responsible invoking configured handlers for the
        specific event, and collecting any events raised as part of handling
        the given event.
        """

        # Invoke all configured handlers with the given event
        for handler in self._event_handlers.get(event.name, []):
            try:
                # Create new UnitOfWork for use in handler
                uow = self._uow_factory.get_uow()
                await handler(uow=uow, event=event)

            except Exception as e:
                # Include the information required to possibly rerun
                # failed handlers if necessary. More needed?
                log = logger.bind(
                    event_id=event.message_id,
                    exception=str(e),
                    traceback=tracebacks.extract(type(e), e, e.__traceback__),
                )
                log.error("raised exception during event handling")

                continue

    async def _handle_command(self, command: Command):
        """Coordinates lifecycle of event handling.

        This method is responsible invoking configured handlers for the specific
        command type, and collecting any events raised as part of handling
        the given command.
        """

        log = logger.bind()

        try:
            # Commands may only have one configured handler
            handler = self._command_handlers[command.name]

        except KeyError as ke_exception:
            log = log.bind(
                missing_key=str(ke_exception),
                configured_handlers=self._command_handlers.keys(),
            )

            log.error("command doesn't have a configured handler")

        try:
            # Create new UnitOfWork and pass to the command handler
            uow = self._uow_factory.get_uow()
            await handler(uow=uow, command=command)

        except Exception as e:
            log = log.bind(
                command_name=command.name,
                command_dict=command.model_dump(),
                exception_type=type(e),
                exception_str=str(e),
            )
            log.error("error handling command")
