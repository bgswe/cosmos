from typing import Iterator

import pytest

from microservices.domain import Aggregate
from microservices.events import Event
from microservices.repository import AsyncRepository
from microservices.unit_of_work import AsyncUnitOfWork, Collector
from microservices.utils import get_logger, uuid4

logger = get_logger()


class MockAggregate(Aggregate):
    """Simple test aggregate implementation."""

    def __init__(self, id: str = None):
        self.id = id if id else str(uuid4())


@pytest.fixture
def mock_aggregate() -> MockAggregate:
    """Simple fixture to provide an instance of MockAggregate."""

    return MockAggregate()


class MockAsyncRepository(AsyncRepository[MockAggregate]):
    """Most basic possible repository to satisfy the need to have one.

    AsyncRepository doesn't have any absolutely required methods. This
    is effectively a NO-OP to have a repository, as AsyncRepository itself
    is an AbstractBaseClass.
    """

    pass


class MockCollector:
    """Simple test implementation of a collector."""

    def collect(self, repository: AsyncRepository) -> Iterator[Event]:
        """Simple test collect that returns the seen aggregates in a new list."""

        return [*repository.seen]


@pytest.fixture
def mock_async_repository() -> AsyncRepository:
    return MockAsyncRepository()


class MockAsyncUnitOfWork:
    def __init__(self, repository: AsyncRepository, collector: Collector):
        """Takes in a repo and a Collector object for use in UnitOfWork."""

        self._repository = repository
        self._collector = collector

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

    def collect_events(self) -> Iterator[Event]:
        """Test implementation of collect_events."""

        return self._collector.collect(repository=self._repository)


@pytest.fixture
def mock_async_unit_of_work(mock_async_repository: AsyncUnitOfWork) -> AsyncUnitOfWork:
    return MockAsyncUnitOfWork(
        repository=mock_async_repository,
        collector=MockCollector(),
    )
