from typing import Callable, Dict, List, Type

from fastapi import APIRouter, FastAPI

from microservices.events import EventStream
from microservices.message_bus import MessageBus, Publisher
from microservices.repository import AsyncRepository, Repository
from microservices.unit_of_work import AsyncUnitOfWork, UnitOfWork, UOWFactory


class InvalidWebhookURL(Exception):
    pass


class InvalidHealthCheckURL(Exception):
    pass


class FastAPIApp(FastAPI):
    """..."""

    def __init__(
        self,
        *args,
        domain: str,
        uow_cls: Type[UnitOfWork],
        command_handlers: Dict[EventStream, Callable] = None,
        event_handlers: Dict[EventStream, List[Callable]] = None,
        event_handlers_external: Dict[EventStream, List[Callable]] = None,
        publisher: Publisher = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        # Initalize mutable kwargs
        if command_handlers is None:
            command_handlers = {}
        if event_handlers is None:
            event_handlers = {}
        if event_handlers_external is None:
            event_handlers_external = {}

        self._domain = domain
        self._uow_cls = uow_cls
        self._command_handlers = command_handlers
        self._event_handlers = event_handlers
        self._event_handlers_external = event_handlers_external
        self._publisher = publisher

        self._health_check_enabled = False
        self._webhook_handler_enabled = False

        self._router = APIRouter(prefix=f"/{domain}")

        self.include_router(self._router)

    @property
    def service_router(self) -> APIRouter:
        return self._router

    def _get_uow_factory(self, repository: Repository) -> UOWFactory:
        """..."""

        return UOWFactory(
            uow_cls=self._uow_cls,
            repository_cls=type(repository),
        )

    def uow(self, repository: AsyncRepository) -> AsyncUnitOfWork:
        return self._get_uow_factory(repository=repository).get_uow()

    def bus(self, repository: AsyncRepository) -> MessageBus:

        return MessageBus(
            uow_factory=self.uow_factory,
            repository=repository,
            command_handlers=self._command_handlers,
            event_handlers=self._event_handlers,
            publisher=self._publisher,
        )
