import asyncio
from typing import Dict, List, Protocol, Type

from microservices.messages import Command, Domain, Event, EventStream, Message
from microservices.unit_of_work import AsyncUnitOfWork, AsyncUnitOfWorkFactory
from microservices.utils import get_logger


class CallbackProtocolWithName(Protocol):
    """Simple interface to define __name__ attr on handlers."""

    __name__: str


class EventHandler(CallbackProtocolWithName):
    """Callback Protocol for an EventHandler function."""

    async def __call__(self, uow: AsyncUnitOfWork, event: Event):
        ...


class CommandHandler(CallbackProtocolWithName):
    """Callback Protocol for a CommandHandler function."""

    async def __call__(self, uow: AsyncUnitOfWork, command: Command):
        ...


logger = get_logger()


class Publisher(Protocol):
    """Object to provide a publish method."""

    def publish(self, event: Event):
        """Publishes internal event to external event stream."""

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
        domain: Domain,
        publisher: Publisher,
        uow_factory: AsyncUnitOfWorkFactory,
        event_handlers: Dict[EventStream, List[EventHandler]] = None,
        command_handlers: Dict[Type[Command], CommandHandler] = None,
    ):
        if event_handlers is None:
            event_handlers = {}

        if command_handlers is None:
            command_handlers = {}

        self._domain = domain
        self._uow_factory = uow_factory
        self._event_handlers = event_handlers
        self._command_handlers = command_handlers
        self._publisher = publisher

    async def handle(self, message: Message):
        """The external interface to send a message through the system.

        This method takes a command or event and invokes the configured handlers
        passed in upon initialization.

        :param: message -> Command or Event entering the system
        """

        # Declares a queue to hold message, and any possibly raised future events
        self._queue = [message]

        # Process queue until all messages are handled and queue is empty
        while self._queue:
            message = self._queue.pop(0)  # first in, first out

            log = logger.bind(message=message)

            # Invoke proper handle method based on message type
            if isinstance(message, Event):
                await self._handle_event(message)
            elif isinstance(message, Command):
                await self._handle_command(message)

            # Hopefully never needed, just in case
            else:
                err_message = "message is not of type Event, nor Command"

                log = logger.bind(err_message=err_message, type=type(message))
                log.error(err_message)

                raise Exception(f"{err_message}, type -> {type(message)}")

    def handle_no_await(self, message: Message):
        """Provides interface to invoke async handle w/o awaiting."""

        loop = asyncio.get_running_loop()
        loop.create_task(self.handle(message=message))

    async def _handle_event(self, event: Event):
        """Coordinates lifecycle of event handling.

        This method is responsible for publishing the event if it is raised
        internally, invoking configured handlers for the specific
        event stream, and collecting any events raised as part of handling
        the given event.

        :param: event -> the given event object to handle
        """

        # Only publish events that originate inside the domain, otherwise
        # we run the chance of republishing a previously published event.
        # Handlers should be idempotent, but it still pollutes message broker.
        if event.domain == self._domain and self._publisher:
            # TODO: What if this fails?
            # Is it okay to commit failed publishes to the DB, and still
            # handle the event internally?
            await self._publisher.publish(event=event)

        # Invoke all configured handlers with the given event
        for handler in self._event_handlers.get(event.stream, []):
            try:
                # Create new UnitOfWork for use in handler
                uow = self._uow_factory.get_uow()
                await handler(uow=uow, event=event)
                # Append all raised events to the message queue
                self._queue.extend(uow.collect_events())

            except Exception as e:
                # Include the information required to possibly rerun
                # failed handlers if necessary. More needed?
                log = logger.bind(
                    domain=event.domain,
                    event_id=event.id,
                    handler_name=handler.__name__,
                    exception=e,
                )
                log.error("raised exception during event handling")

                continue

    async def _handle_command(self, command: Command):
        """Coordinates lifecycle of event handling.

        This method is responsible invoking configured handlers for the specific
        command type, and collecting any events raised as part of handling
        the given command.

        :param: command -> the given event object to handle
        """

        try:
            # Commands may only have one configured handler
            handler = self._command_handlers[type(command)]
            # Create new UnitOfWork and pass to the command handler
            uow = self._uow_factory.get_uow()
            await handler(uow=uow, command=command)
            # Append all raised events to the message queue
            self._queue.extend(uow.collect_events())

        except Exception:
            # Include the information required to possibly rerun
            # failed handlers if necessary. More needed?
            log = logger.bind(
                command_dict=command.dict(),
                handler_name=handler.__name__,
            )
            log.error("raised exception during command handling")
