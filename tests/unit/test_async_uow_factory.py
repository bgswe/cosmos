from typing import TypeVar

import pytest
from structlog import get_logger

from cosmos.domain import AggregateRoot
from cosmos.repository import AsyncRepository
from cosmos.unit_of_work import AsyncUnitOfWork, AsyncUnitOfWorkFactory
from tests.conftest import MockAsyncRepository, MockAsyncUnitOfWork

T = TypeVar("T", bound=AggregateRoot)


logger = get_logger()


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
    )

    assert uow_factory is not None


@pytest.fixture
def uow_factory() -> AsyncUnitOfWorkFactory:
    return AsyncUnitOfWorkFactory(
        uow_cls=MockAsyncUnitOfWork, uow_kwargs={"repository": MockAsyncRepository()}
    )


# NOTE: Test violates mypy rule, revisit
# def test_async_uow_factory_get_uow_returns_uow(uow_factory: AsyncUnitOfWorkFactory):
#     """Verifies AsyncUnitOfWorkFactory get_uow returns an AsyncUnitOfWork."""

#     uow = uow_factory.get_uow()

#     assert issubclass(type(uow), AsyncUnitOfWork)


async def test_async_uow_factory_get_uow_returns_uow_with_valid_repo(
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
