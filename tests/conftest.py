from __future__ import annotations

from typing import Iterable
from uuid import UUID

import pytest

from cosmos.domain import AggregateRoot, Entity, Event
from cosmos.repository import AsyncRepository
from cosmos.unit_of_work import Collect
from cosmos.utils import get_logger

logger = get_logger()


@pytest.fixture()
def mock_uuid() -> UUID:
    """Returns a known mock UUID for use when it's needed to be a specific value."""

    return UUID("11111111-1111-1111-1111-111111111111")


class MockAggregate(AggregateRoot):
    """Simple test aggregate implementation."""

    @classmethod
    def create(cls, pk: UUID = None) -> MockAggregate:
        new = Entity.create_entity(cls=cls, pk=pk)

        assert type(new) == MockAggregate  # mypy assertion

        return new


@pytest.fixture
def mock_aggregate() -> MockAggregate:
    """Simple fixture to provide an instance of MockAggregate."""

    return MockAggregate.create()


class MockAEvent(Event):
    stream = "MockA"


@pytest.fixture
def mock_a_event() -> Event:
    return MockAEvent()


class MockBEvent(Event):
    stream = "MockB"


@pytest.fixture
def mock_b_event() -> Event:
    return MockBEvent()


class MockCEvent(Event):
    stream = "MockC"


@pytest.fixture
def mock_c_event() -> Event:
    return MockCEvent()


class MockAsyncRepository(AsyncRepository[MockAggregate]):
    """Most basic possible repository to satisfy the need to have one.

    AsyncRepository doesn't have any absolutely required methods. This
    is effectively a NO-OP to have a repository, as AsyncRepository itself
    is an AbstractBaseClass.
    """

    pass


def mock_collect(repository: AsyncRepository) -> Iterable[Event]:
    """Simple test collect that returns the seen aggregates in a new list."""

    return [*repository.seen]


@pytest.fixture
def mock_async_repository() -> AsyncRepository:
    return MockAsyncRepository()


class MockAsyncUnitOfWork:
    def __init__(self, repository: AsyncRepository, collect: Collect):
        """Takes in a repo and a Collector object for use in UnitOfWork."""

        self._repository = repository
        self._collect = collect

    async def __aenter__(self):
        """Simple test implementation."""

        logger.debug("MockAsyncUnitOfWork.__aenter__")

    async def __aexit__(self, *args):
        """Simple test implementation."""

        logger.debug("MockAsyncUnitOfWork.__aexit__")

    @property
    def repository(self) -> AsyncRepository:
        """Getter for the repository instance."""

        return self._repository

    def collect_events(self) -> Iterable[Event]:
        """Test implementation of collect_events."""

        return self._collect(repository=self._repository)


@pytest.fixture
def mock_async_unit_of_work(
    mock_async_repository: AsyncRepository,
) -> MockAsyncUnitOfWork:
    return MockAsyncUnitOfWork(
        repository=mock_async_repository,
        collect=mock_collect,
    )
