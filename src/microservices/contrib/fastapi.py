from typing import Callable, Dict, List, Type

from fastapi import FastAPI

from microservices.domain import Event
from microservices.message_bus import MessageBus, Publisher
from microservices.repository import AsyncRepository
from microservices.unit_of_work import AsyncUnitOfWork


class FastAPIApp(FastAPI):
    def __init__(
        self,
        *args,
        uow_cls: AsyncUnitOfWork,
        publisher: Publisher,
        command_handlers: Dict[Type[Event], Callable],
        event_handlers: Dict[Type[Event], List[Callable]],
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self._uow_cls = uow_cls
        self._command_handlers = command_handlers
        self._event_handlers = event_handlers
        self._publisher = publisher

    def uow(self, repository: AsyncRepository) -> AsyncUnitOfWork:
        return self._uow_cls(repository=repository)

    def bus(self, repository: AsyncRepository) -> MessageBus:
        return MessageBus(
            uow_cls=self._uow_cls,
            repository=repository,
            command_handlers=self._command_handlers,
            event_handlers=self._event_handlers,
            publisher=self._publisher,
        )
