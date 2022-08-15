import asyncio
from typing import Callable, Dict, List, Protocol, Type, Union

from microservices.domain import Command
from microservices.events import Domain, Event, EventStream
from microservices.unit_of_work import AsyncUOWFactory

Message = Union[Command, Event]


class Publisher(Protocol):
    """Object to provide a publish method."""

    def publish(self, event: Event):
        """Publishes internal event to external event stream."""

        ...


class _MessageBus:
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
        uow_factory: AsyncUOWFactory,
        event_handlers: Dict[EventStream, List[Callable]] = None,
        command_handlers: Dict[Type[Command], Callable] = None,
        publisher: Publisher = None,
    ):
        assert event_handlers is not None and command_handlers is not None

        if event_handlers is None:
            assert (
                command_handlers.keys() > 0
            )  # ensure given handler config isn't empty
            event_handlers = {}

        if command_handlers is None:
            assert event_handlers.keys() > 0  # ensure given handler config isn't empty
            command_handlers = {}

        self.domain = domain
        self.uow_factory = uow_factory
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers
        self.publisher = publisher

    async def handle(self, message: Message):
        self.queue = [message]

        while self.queue:
            message = self.queue.pop(0)

            if isinstance(message, Event):
                await self.handle_event(message)
            elif isinstance(message, Command):
                await self.handle_command(message)
            else:
                raise Exception(f"{message} was not an Event or Command")

    async def handle_event(self, event: Event):
        # Only publish events that originate inside the domain, otherwise
        # we run the chance of republishing the a previously published event.
        # Handlers should be idempotent, but it still pollutes message broker.
        if event.stream.value.split(".")[0] == self.domain and self.publisher:
            # TODO: What if this fails?
            await self.publisher.publish(event=event)

        for handler in self.event_handlers[event.stream]:
            try:
                uow = self.uow_factory.get_uow()
                await handler(uow=uow, event=event)
                self.queue.extend(uow.collect_events())

            except Exception:
                # logger.exception("Exception handling event %s", event)
                continue

    async def handle_command(self, command: Command):
        # logger.debug("handling command %s", command)
        try:
            handler = self.command_handlers[type(command)]
            uow = self.uow_factory.get_uow()
            await handler(uow=uow, command=command)
            self.queue.extend(uow.collect_events())

        except Exception:
            # logger.exception("Exception handling command %s", command)
            raise


class MessageBus(_MessageBus):
    def handle_no_await(self, message: Message):
        """Provides way to invoke async handle w/o awaiting."""

        loop = asyncio.get_running_loop()
        loop.create_task(super().handle(message=message))
