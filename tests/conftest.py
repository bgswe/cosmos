from __future__ import annotations

from typing import Iterable
from uuid import UUID

import pytest
from structlog import get_logger  # noqa

from cosmos.domain import AggregateRoot, Entity, Event
from cosmos.repository import AsyncRepository
from cosmos.unit_of_work import AsyncUnitOfWork

logger = get_logger()


@pytest.fixture()
def mock_uuid() -> UUID:
    """Returns a known mock UUID for use when it's needed to be a specific value"""

    return UUID("11111111-1111-1111-1111-111111111111")


class MockAggregate(AggregateRoot):
    """Simple test aggregate implementation"""

    @classmethod
    def create(cls, id: UUID|None = None) -> MockAggregate:
        new = Entity.create_entity(cls=cls, id=id)

        assert type(new) == MockAggregate  # mypy assertion

        return new


@pytest.fixture
def mock_aggregate() -> MockAggregate:
    """Simple fixture to provide an instance of MockAggregate"""

    return MockAggregate.create()


class MockEventA(Event):
    ...


@pytest.fixture
def mock_event_a() -> Event:
    return MockEventA()


class MockEventB(Event):
    ...


@pytest.fixture
def mock_b_event() -> Event:
    return MockEventB()


class MockEventC(Event):
    ...


@pytest.fixture
def mock_c_event() -> Event:
    return MockEventC()


class MockAsyncRepository(AsyncRepository[MockAggregate]):
    """Most basic possible repository to satisfy the need to have one

    AsyncRepository doesn't have any absolutely required methods. This
    is effectively a NO-OP to have a repository, as AsyncRepository itself
    is an AbstractBaseClass.
    """

    pass


def mock_collect(repository: AsyncRepository) -> Iterable[Event]:
    """Simple test collect that returns the seen aggregates in a new list"""

    return [*repository.seen]


@pytest.fixture
def mock_async_repository() -> AsyncRepository:
    return MockAsyncRepository()


class MockAsyncUnitOfWork(AsyncUnitOfWork):
    def __init__(self, repository: AsyncRepository):
        """Takes in a repo and a Collector object for use in UnitOfWork"""

        self.repository = repository

    async def __aenter__(self):
        """Simple test implementation"""

        logger.debug("MockAsyncUnitOfWork.__aenter__")

    async def __aexit__(self, *args):
        """Simple test implementation"""

        logger.debug("MockAsyncUnitOfWork.__aexit__")


@pytest.fixture
def mock_async_unit_of_work(
    mock_async_repository: AsyncRepository,
) -> MockAsyncUnitOfWork:
    return MockAsyncUnitOfWork(
        repository=mock_async_repository,
    )
