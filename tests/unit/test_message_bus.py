from __future__ import annotations

from typing import Iterable, List, Tuple

import pytest
from structlog import get_logger

from cosmos.domain import Event
from cosmos.message_bus import EventHandler, MessageBus
from cosmos.repository import AsyncRepository
from cosmos.unit_of_work import AsyncUnitOfWork, AsyncUnitOfWorkFactory, Collect

from tests.conftest import (
    MockAsyncRepository,
    MockAsyncUnitOfWork,
    mock_collect,
    MockEventA,
    MockEventB,
)

logger = get_logger()


async def mock_publish(event: Event):
    logger.bind(event_dict=event.dict())

    logger.debug("MockPublisher.publish")


@pytest.fixture
def empty_message_bus() -> MessageBus:
    return MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
    )


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

            log = logger.bind(event_dict=event.dict())
            log.debug("mock_event_handler invoked")

        return was_invoked, mock_event_handler


@pytest.fixture
def mock_event_handler_factory() -> MockEventHandlerFactory:
    """Simple fixture to expose the MockEventHandlerFactory."""

    return MockEventHandlerFactory()


def test_message_bus_most_basic_initialization_doesnt_raise_exception():
    """MessageBus requires at minimum domain, and a UnitOfWorkFactory."""

    MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
    )


async def test_message_bus_without_event_handlers_doesnt_raise_exception_on_handle(
    empty_message_bus: MessageBus,
    mock_event_a: Event,
):
    """Verify calling handle with on a bus w/o handlers doesn't raise exception."""

    await empty_message_bus.handle(mock_event_a)


async def test_message_bus_event_with_alternate_event_handler_doesnt_invoke_handler(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verifies calling handle on bus w/ only other events configured w/ handlers."""

    invocation_flag, handler = mock_event_handler_factory.get()

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        # mock_event is from stream MockA, so we handle MockB only
        event_handlers={"MockBEvent": [handler]},
    )

    await bus.handle(mock_event_a)

    # Ensure the handler was not invoked incorrectly
    assert not invocation_flag.invoked


async def test_message_bus_simple_event_handler_invokes_correct_handler(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verify calling handle on bus w/ configured handler, invokes said handler."""

    invocation_flag, handler = mock_event_handler_factory.get()

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        event_handlers={"MockEventA": [handler]},
    )

    await bus.handle(mock_event_a)

    assert invocation_flag.invoked


async def test_message_bus_multiple_event_handlers_invokes_list_of_handlers(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verify calling handle on bus w/ many configured handlers, invokes them all."""

    invocation_flag_a, handler_a = mock_event_handler_factory.get()
    invocation_flag_b, handler_b = mock_event_handler_factory.get()

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        event_handlers={"MockEventA": [handler_a, handler_b]},
    )

    await bus.handle(mock_event_a)

    assert invocation_flag_a.invoked
    assert invocation_flag_b.invoked


async def test_message_bus_event_handler_invokes_only_associated_handlers(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
):
    """Verifies ONLY the associated event handlers are invoked w/ many handlers."""

    event_flags = {}
    event_handlers = {}

    events = ["MockEventA", "AltMockEventA", "AltMockEventB"]

    for event in events:
        handlers = [mock_event_handler_factory.get() for _ in range(3)]

        event_flags[event] = [handler[0] for handler in handlers]
        event_handlers[event] = [handler[1] for handler in handlers]

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        event_handlers=event_handlers,
    )

    await bus.handle(mock_event_a)

    # Invokes the list of handlers for proper event
    for flag in event_flags["MockEventA"]:
        assert flag.invoked

    # Does not invoke other handlers for other events
    for event in ["AltMockEventA", "AltMockEventB"]:
        for flag in event_flags[event]:
            assert not flag.invoked


@pytest.fixture
def mock_collect_spoofed_event(mock_b_event: Event) -> Collect:
    first = True

    def mock_collect(repository: AsyncRepository) -> Iterable[Event]:
        """Simple test collect that returns the seen aggregates in a new list."""

        # NOTE: This is a relatively easy way to spoof a handler raising an event
        # thorough handling. There may be other/better ways to do this, possibly
        # through the repo itself.
        nonlocal first
        if first:
            first = False

            return [mock_b_event]

        return [*repository.seen]

    return mock_collect


async def test_message_bus_calls_handler_for_event_raised_in_first_handler(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
    mock_collect_spoofed_event: Collect,
):
    """Verifies the message bus correctly handles a message's downstream events."""

    mock_b_handler_invoked, mock_b_handler = mock_event_handler_factory.get()

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        event_handlers={
            # We don't require the invocation flag for this event, just grab a handler
            "MockEventA": [mock_event_handler_factory.get()[1]],
            "MockEventB": [mock_b_handler],
        },
    )

    await bus.handle(mock_event_a)
    # We want to verify that the spoofed MockB event is correctly handled, and
    # the configured handler invoked
    assert mock_b_handler_invoked.invoked


@pytest.fixture
def mock_collect_spoofed_event_sequence(
    mock_b_event: Event,
    mock_c_event: Event,
) -> Collect:
    count = 0

    def mock_collect(repository: AsyncRepository) -> Iterable[Event]:
        """Simple test collect that returns the seen aggregates in a new list."""

        # NOTE: This is a relatively easy way to spoof a handler raising an event
        # thorough handling. There may be other/better ways to do this, possibly
        # through the repo itself.
        nonlocal count

        match count:
            case 0:
                count += 1
                return [mock_b_event]  # simulate MockB raised in MockA handler
            case 1:
                count += 1
                return [mock_c_event]  # simulate MockC raised in MockB handler

        return [*repository.seen]

    return mock_collect


async def test_message_bus_handle_calls_correct_event_sequence(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
    mock_collect_spoofed_event_sequence: Collect,
):
    """..."""

    mock_b_handler_invoked, mock_b_handler = mock_event_handler_factory.get()
    mock_c_handler_invoked, mock_c_handler = mock_event_handler_factory.get()

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        event_handlers={
            # We don't require the invocation flag for this event, just grab a handler
            "MockEventA": [mock_event_handler_factory.get()[1]],
            "MockEventB": [mock_b_handler],
            "MockEventC": [mock_c_handler],
        },
    )

    seq = await bus.handle(mock_event_a)  # noqa
    # TODO: Finish this test to ensure that events are properly being ordered

    assert mock_b_handler_invoked.invoked
    assert mock_c_handler_invoked.invoked


@pytest.fixture
def mock_collect_spoofed_event_sequence_many() -> Tuple[List[Event], Collect]:
    count = 0

    events = [MockEventA(), MockEventB(), MockEventB(), MockEventA()]

    def mock_collect(repository: AsyncRepository) -> Iterable[Event]:
        """Simple test collect that returns the seen aggregates in a new list."""

        # NOTE: This is a relatively easy way to spoof a handler raising an event
        # thorough handling. There may be other/better ways to do this, possibly
        # through the repo itself.
        nonlocal count

        match count:
            case 0:
                count += 1
                return [events[0]]  # simulate MockB raised in MockA handler
            case 1:
                count += 1
                return [events[1]]  # simulate MockC raised in MockB handler
            case 2:
                count += 1
                return [events[2]]  # simulate MockB raised in MockA handler
            case 3:
                count += 1
                return [events[3]]  # simulate MockC raised in MockB handler

        return [*repository.seen]

    return events, mock_collect


async def test_message_bus_handle_calls_correct_event_sequence_many(
    mock_event_a: Event,
    mock_event_handler_factory: MockEventHandlerFactory,
    mock_collect_spoofed_event_sequence_many: Tuple[List[Event], Collect],
):
    """..."""

    _, mock_a_handler = mock_event_handler_factory.get()
    _, mock_b_handler = mock_event_handler_factory.get()
    mock_c_handler_invoked, mock_c_handler = mock_event_handler_factory.get()

    mock_events = mock_collect_spoofed_event_sequence_many

    bus = MessageBus(
        event_publish=mock_publish,
        uow_factory=AsyncUnitOfWorkFactory(
            uow_cls=MockAsyncUnitOfWork,
            repository_cls=MockAsyncRepository,
        ),
        event_handlers={
            # We don't require the invocation flag for this event, just grab a handler
            "MockEventA": [mock_a_handler],
            "MockEventB": [mock_b_handler],
            "MockEventC": [mock_c_handler],
        },
    )

    seq = await bus.handle(mock_event_a)

    logger.debug("handled seq", seq=seq)

    # No C events raised, verify not invoked
    assert not mock_c_handler_invoked.invoked

    # Iterate through spoofed sequence of raised events,
    # Verify the returned sequence from handle matches
    for i, e in enumerate([mock_event_a, *mock_events]):
        assert e.message_id.hex == seq[i].hex
