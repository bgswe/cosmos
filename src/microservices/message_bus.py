import asyncio
from abc import abstractmethod
from typing import Callable, Dict, List, Protocol, Type, Union

from microservices.domain import Command
from microservices.events import Event, EventStream
from microservices.unit_of_work import UnitOfWorkFactory

Message = Union[Command, Event]


class Publisher(Protocol):
    @abstractmethod
    def publish(self, event: Event):
        raise NotImplementedError


class _MessageBus:
    def __init__(
        self,
        domain: str,
        uow_factory: UnitOfWorkFactory,
        event_handlers: Dict[EventStream, List[Callable]] = None,
        command_handlers: Dict[Type[Command], Callable] = None,
        publisher: Publisher = None,
    ):
        if event_handlers is None:
            event_handlers = {}

        if command_handlers is None:
            command_handlers = {}

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
        # TODO: What if this fails?
        if self.publisher:
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
