import asyncio
from abc import abstractmethod
from typing import Callable, Dict, List, Protocol, Type, Union

from microservices.domain import Command, Event
from microservices.repository import Repository
from microservices.unit_of_work import UnitOfWork

Message = Union[Command, Event]


class Publisher(Protocol):
    @abstractmethod
    def publish(self, event: Event) -> None:
        raise NotImplementedError


class _MessageBus:
    def __init__(
        self,
        uow_cls: Type[UnitOfWork],
        repository: Repository,
        event_handlers: Dict[Type[Event], List[Callable]] = None,
        command_handlers: Dict[Type[Command], Callable] = None,
        publisher: Publisher = None,
    ):
        if event_handlers is None:
            event_handlers = {}

        if command_handlers is None:
            command_handlers = {}

        self.uow_cls = uow_cls
        self.repo = repository
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers
        self._publisher = publisher

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
        for handler in self.event_handlers[event.type]:
            try:
                uow = self.uow_cls(repository=self.repo)
                await handler(uow=uow, event=event)

                # TODO: What if this fails?
                if self._publisher:
                    await self._publisher.publish(event=event)

                self.queue.extend(uow.collect_events())
            except Exception:
                # logger.exception("Exception handling event %s", event)
                continue

    async def handle_command(self, command: Command):
        # logger.debug("handling command %s", command)
        try:
            handler = self.command_handlers[type(command)]
            uow = self.uow_cls(repository=self.repo)
            await handler(uow=uow, command=command)
            self.queue.extend(uow.collect_events())
        except Exception:
            # logger.exception("Exception handling command %s", command)
            raise


class MessageBus(_MessageBus):
    def handle(self, message: Message):
        """Provides way to invoke async handle w/o awaiting.

        TODO: Execute message validation if it is command. Raise
        exception synchronously if command is not valid.
        """

        loop = asyncio.get_running_loop()
        loop.create_task(super().handle(message=message))
