from typing import Generic, Iterator, TypeVar

import pytest
from conftest import MockAsyncRepository

from microservices.domain import Aggregate
from microservices.events import Event
from microservices.repository import AsyncRepository
from microservices.unit_of_work import AsyncUnitOfWork, AsyncUnitOfWorkFactory
from microservices.utils import get_logger

T = TypeVar("T", bound=Aggregate)


logger = get_logger()


class MockAsyncUnitOfWork(Generic[T]):
    def __init__(self, repository: AsyncRepository[T]) -> None:
        """Ensure implementation takes an AsyncRepository on init."""

        self._repository = repository

    @property
    def repository(self) -> AsyncRepository:
        return self._repository

    def collect_events(self) -> Iterator[Event]:
        pass

    async def __aenter__(self):
        return None

    async def __aexit__(self, *args):
        logger.msg(args)


@pytest.fixture
def mock_async_unit_of_work(mock_async_repository: AsyncUnitOfWork) -> AsyncUnitOfWork:
    return MockAsyncUnitOfWork(repository=mock_async_repository)


async def test_mock_async_uow_is_valid_context_manager(
    mock_async_unit_of_work: AsyncUnitOfWork,
):
    """Verify as a baseline MockAsyncUnitOfWork is a valid context manager."""

    async with mock_async_unit_of_work as thing:
        assert thing is None


async def test_async_uow_factory_initializes():
    """Verifies AsyncUnitOfWorkFactory is initialized w/o issue."""

    uow_factory = AsyncUnitOfWorkFactory(
        uow_cls=MockAsyncUnitOfWork,
        repository_cls=MockAsyncRepository,
    )

    assert uow_factory is not None


@pytest.fixture
def uow_factory() -> AsyncUnitOfWorkFactory:
    return AsyncUnitOfWorkFactory(
        uow_cls=MockAsyncUnitOfWork,
        repository_cls=MockAsyncRepository,
    )


def test_async_uow_factory_get_uow_returns_uow(uow_factory: AsyncUnitOfWorkFactory):
    """Verifies AsyncUnitOfWorkFactory get_uow returns an AsyncUnitOfWork."""

    uow = uow_factory.get_uow()

    assert issubclass(type(uow), AsyncUnitOfWork)


def test_async_uow_factory_get_uow_returns_uow_with_valid_repo(
    uow_factory: AsyncUnitOfWorkFactory,
):
    """Verifies AsyncUnitOfWorkFactory get_uow returns uow w/ valid AsyncRepository."""

    uow = uow_factory.get_uow()

    assert isinstance(uow.repository, AsyncRepository)


async def test_async_uow_factory_get_uow_returns_valid_uow(
    uow_factory: AsyncUnitOfWorkFactory,
):
    """Verify the returned uow is a valid context manager."""

    async with uow_factory.get_uow() as thing:
        assert thing is None
