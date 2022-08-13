from typing import Callable, Dict, List, Type

from fastapi import APIRouter, FastAPI

from microservices.events import EventStream
from microservices.message_bus import MessageBus, Publisher
from microservices.repository import AsyncRepository, Repository
from microservices.unit_of_work import AsyncUnitOfWork, AsyncUOWFactory, UnitOfWork


class FastAPIApp(FastAPI):
    """..."""

    def __init__(
        self,
        *args,
        domain: str,
        uow_cls: Type[UnitOfWork],
        command_handlers: Dict[EventStream, Callable] = None,
        event_handlers: Dict[EventStream, List[Callable]] = None,
        publisher: Publisher = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Initalize mutable kwargs
        if command_handlers is None:
            command_handlers = {}
        if event_handlers is None:
            event_handlers = {}

        self._domain = domain
        self._uow_cls = uow_cls
        self._command_handlers = command_handlers
        self._event_handlers = event_handlers
        self._publisher = publisher

        self._router = APIRouter(prefix=f"/{domain}")
        self._router_included = False

    @property
    def service_router(self) -> APIRouter:
        """..."""

        return self._router

    def include_service_router(self):
        """..."""

        if not self._router_included:
            self.include_router(self._router)
            self._router_included = True

    def _get_uow_factory(self, repository: Repository) -> AsyncUOWFactory:
        """..."""

        return AsyncUOWFactory(
            uow_cls=self._uow_cls,
            repository_cls=type(repository),
        )

    def uow(self, repository: AsyncRepository) -> AsyncUnitOfWork:
        """..."""

        return self._get_uow_factory(repository=repository).get_uow()

    def bus(self, repository: AsyncRepository) -> MessageBus:
        """..."""

        return MessageBus(
            domain=self._domain,
            uow_factory=self._get_uow_factory(repository=repository),
            command_handlers=self._command_handlers,
            event_handlers=self._event_handlers,
            publisher=self._publisher,
        )
