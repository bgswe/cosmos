from __future__ import annotations

from typing import Tuple

import pytest

from microservices.events import Event, EventStream
from microservices.message_bus import EventHandler, MessageBus
from microservices.unit_of_work import AsyncUnitOfWork, AsyncUnitOfWorkFactory
from microservices.utils import get_logger
from tests.conftest import MockAsyncRepository, MockAsyncUnitOfWork, MockCollector

logger = get_logger()


class MockPublisher:
    """Simple test implementation of a publisher."""

    def publish(self, event: Event):
        logger.bind(event_dict=event.dict())

        logger.debug("MockPublisher.publish")


@pytest.fixture
def empty_message_bus() -> MessageBus:
    return MessageBus(
        domain="test",
        publisher=MockPublisher,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
            collector=MockCollector(),
        ),
    )


class MockEvent(Event):
    stream = EventStream.MockA


@pytest.fixture
def mock_event() -> Event:
    return MockEvent()


class MockEventHandlerInvokationFlag:
    def __init__(self):
        self._invoked = False

    def mark_invoked(self):
        self._invoked = True

    @property
    def invoked(self) -> bool:
        return self._invoked


class MockEventHandlerFactory:
    """Generates a mocked event handler w/ an associated invocation flag."""

    def get(self) -> Tuple[MockEventHandlerInvokationFlag, EventHandler]:
        """Returns a new handler/flag pair."""

        was_invoked = MockEventHandlerInvokationFlag()

        async def mock_event_handler(uow: AsyncUnitOfWork, event: Event):
            """Simple event handler wh/ logs the event and marks flag as invoked."""

            was_invoked.mark_invoked()

            log = logger.bind(event_dict=mock_event.dict())
            log.debug("mock_event_handler invoked")

        return was_invoked, mock_event_handler


@pytest.fixture
def mock_event_handler_factory() -> MockEventHandlerFactory:
    """Simple fixture to expose the MockEventHandlerFactory."""

    return MockEventHandlerFactory()


def test_message_bus_most_basic_initialization_doesnt_raise_exception():
    """MessageBus requires at minimum domain, and a UnitOfWorkFactory."""

    MessageBus(
        domain="test",
        publisher=MockPublisher,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
            collector=MockCollector(),
        ),
    )


async def test_message_bus_without_event_handlers_doesnt_raise_exception_on_handle(
    empty_message_bus: MessageBus,
    mock_event: Event,
):
    """Verify calling handle with on a bus w/o handlers doesn't raise exception."""

    await empty_message_bus.handle(mock_event)


async def test_message_bus_event_with_alternate_event_handler_doesnt_invoke_handler(
    mock_event: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verifies calling handle on bus w/ only other events configured w/ handlers."""

    invocation_flag, handler = mock_event_handler_factory.get()

    bus = MessageBus(
        domain="test",
        publisher=MockPublisher,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
            collector=MockCollector(),
        ),
        # mock_event is from stream MockA, so we handle MockB only
        event_handlers={EventStream.MockB: [handler]},
    )

    await bus.handle(mock_event)

    # Ensure the handler was not invoked incorrectly
    assert not invocation_flag.invoked


async def test_message_bus_simple_event_handler_invokes_correct_handler(
    mock_event: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verify calling handle on bus w/ configured handler, invokes said handler."""

    invocation_flag, handler = mock_event_handler_factory.get()

    bus = MessageBus(
        domain="test",
        publisher=MockPublisher,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
            collector=MockCollector(),
        ),
        event_handlers={EventStream.MockA: [handler]},
    )

    await bus.handle(mock_event)

    assert invocation_flag.invoked


async def test_message_bus_multiple_event_handlers_invokes_list_of_handlers(
    mock_event: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verify calling handle on bus w/ many configured handlers, invokes them all."""

    invocation_flag_a, handler_a = mock_event_handler_factory.get()
    invocation_flag_b, handler_b = mock_event_handler_factory.get()

    bus = MessageBus(
        domain="test",
        publisher=MockPublisher,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
            collector=MockCollector(),
        ),
        event_handlers={EventStream.MockA: [handler_a, handler_b]},
    )

    await bus.handle(mock_event)

    assert invocation_flag_a.invoked
    assert invocation_flag_b.invoked


async def test_message_bus_event_handler_invokes_only_associated_handlers(
    mock_event: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verifies ONLY the associated event handlers are invoked w/ many handlers."""

    event_flags = {}
    event_handlers = {}

    events = [EventStream.MockA, EventStream.MockB, EventStream.MockC]

    for event in events:
        handlers = [mock_event_handler_factory.get() for _ in range(3)]

        event_flags[event] = [handler[0] for handler in handlers]
        event_handlers[event] = [handler[1] for handler in handlers]

    bus = MessageBus(
        domain="test",
        publisher=MockPublisher,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
            collector=MockCollector(),
        ),
        event_handlers=event_handlers,
    )

    await bus.handle(mock_event)

    # Invokes the list of handlers for proper event
    for flag in event_flags[mock_event.stream]:
        assert flag.invoked

    # Does not invoke other handlers for other events
    for event in [EventStream.MockB, EventStream.MockC]:
        for flag in event_flags[event]:
            assert not flag.invoked
