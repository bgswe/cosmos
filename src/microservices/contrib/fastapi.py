from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, List, Type

import aiohttp
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

from microservices.domain import Event, EventStreams, Subscription
from microservices.message_bus import MessageBus, Publisher
from microservices.repository import AsyncRepository
from microservices.unit_of_work import AsyncUnitOfWork


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
        uow_cls: Type[AsyncUnitOfWork],
        command_handlers: Dict[EventStreams, Callable] = None,
        event_handlers: Dict[EventStreams, List[Callable]] = None,
        event_handlers_external: Dict[EventStreams, List[Callable]] = None,
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

    def uow(self, repository: AsyncRepository) -> AsyncUnitOfWork:
        return self._uow_cls(repository=repository)

    def bus(
        self,
        repository: AsyncRepository,
        external: bool = False,
    ) -> MessageBus:
        ch = None
        eh = None
        pub = None

        if external:
            eh = self._event_handlers_external
        else:
            ch = self._command_handlers
            eh = self._event_handlers
            pub = self._publisher

        return MessageBus(
            uow_cls=self._uow_cls,
            repository=repository,
            command_handlers=ch,
            event_handlers=eh,
            publisher=pub,
        )

    async def subscribe(
        self,
        service_origin: str,
        sub_list: List[str],
        repository: AsyncRepository,
        webhook_url: str = None,
        health_check_url: str = None,
    ) -> None:
        if not len(sub_list):
            return None

        # For webhook and healthcheck check, if there is no provided value,
        # check whether the default handlers have been been enabled. If so,
        # use default URL, otherwise raise an exception
        if webhook_url is None:
            if not self._webhook_handler_enabled:
                raise InvalidWebhookURL
            else:
                webhook_url = f"{service_origin}/{self._domain}/health"

        if health_check_url is None:
            if not self._health_check_enabled:
                raise InvalidHealthCheckURL
            else:
                health_check_url = f"{service_origin}/{self._domain}/events"

        uow = self._uow_cls(repository=repository)

        # Responsible for management of subscription persistence
        async with uow:
            # Get the list of current subs, and map them to the stream
            currently_subbed_list = await uow.repository.get_list()
            currently_subbed_list = {s.stream for s in currently_subbed_list}

            # Check if amount existing subs matches configuration
            # if so, it's a NO-OP, otherwise remediate
            if len(sub_list) != len(currently_subbed_list):
                for stream in sub_list - currently_subbed_list:
                    sub = Subscription.create(
                        stream=stream,
                        webhook_url=webhook_url,
                        health_check_url=health_check_url,
                    )

                    await uow.repository.add(sub)

                    async with aiohttp.ClientSession() as session:
                        async with session.post(
                            "{service_origin}/event-stream/subscribe",
                            json={
                                "id": sub.id,
                                "stream": sub.stream,
                                "webhookURL": sub.webhook_url,
                                "healthCheckURL": sub.health_check_url,
                            },
                        ) as resp:
                            if resp.status != 200:
                                # TODO: Need this uow tx to fail
                                raise Exception

    def use_health_check(self) -> None:
        if self._health_check_enabled:
            return None

        self._health_check_enabled = True

        # NOTE: Look into better way of doing this?
        # I wonder if you can use a function as a value,
        # wh/ would allow you to define this function elsewhere.
        @self.get(f"/{self._domain}/health", status_code=200)
        async def health_check():
            # TODO: Research into health check best practices to answer
            # questions. What is/are the situation(s) that would yield
            # an un-healthy status, outside the situation in which the
            # service is down.
            return {"status": "HEALTHY"}

    def use_event_webhook(
        self, *, repository: AsyncRepository, webhook_path: str = None
    ) -> None:
        if self._webhook_handler_enabled:
            return None

        self._webhook_handler_enabled = True

        class EventStreamSchema(BaseModel):
            stream: str
            messages: List[Dict[str, Any]]

        # Use supplied webhook path, or the default
        @self.post(webhook_path or f"/{self._domain}/events")
        async def webhook_handler(events: EventStreamSchema) -> None:
            bus = self.bus(repository=repository, external=True)

            for m in events.messages:

                @dataclass
                class TempEvent(Event):
                    stream: ClassVar[str] = events.stream
                    values: Dict[str, Any]

                bus.handle(TempEvent(values=m))
