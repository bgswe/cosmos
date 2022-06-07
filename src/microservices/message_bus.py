import asyncio
from typing import Callable, Dict, List, Type, Union

from microservices.domain import Command, Event
from microservices.unit_of_work import UnitOfWork


Message = Union[Command, Event]


class _MessageBus:
    def __init__(
        self,
        uow: UnitOfWork,
        event_handlers: Dict[Type[Event], List[Callable]],
        command_handlers: Dict[Type[Command], Callable],
    ):
        self.uow = uow
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers

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
        for handler in self.event_handlers[type(event)]:
            try:
                # logger.debug("handling event %s with handler %s", event, handler)
                await handler(uow=self.uow, event=event)
                self.queue.extend(self.uow.collect_events())
            except Exception:
                # logger.exception("Exception handling event %s", event)
                continue

    async def handle_command(self, command: Command):
        # logger.debug("handling command %s", command)
        try:
            handler = self.command_handlers[type(command)]
            await handler(uow=self.uow, command=command)
            self.queue.extend(self.uow.collect_events())
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
