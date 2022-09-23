from distutils.command.install_egg_info import safe_name
from typing import List
from uuid import UUID

import pytest

from microservices.domain import Aggregate
from microservices.repository import AsyncRepository
from microservices.utils import get_logger
from tests.conftest import MockAggregate

logger = get_logger()


class MockAsyncRepositoryGet(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository with '_get' method."""

    async def _get(self, pk: UUID) -> MockAggregate:
        """Simulates returning an aggregate or None if 'Not Found'."""

        if pk == UUID(
            "11111111-1111-1111-1111-111111111111"
        ):  # pass this str as id to simulate Not Found
            return None

        return MockAggregate(pk=pk)  # otherwise, simulate a found aggregate


@pytest.fixture
def mock_async_repository_get() -> AsyncRepository:
    """Simple Fixture to provide an instance of the 'get' mock repository."""

    return MockAsyncRepositoryGet()


class MockAsyncRepositoryGetList(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository with '_get_list' method."""

    async def _get_list(self, **kwargs) -> List[Aggregate]:
        """Simulates returning a list of aggregates."""

        if kwargs.get("empty", None):
            return []

        return [MockAggregate.create() for _ in range(5)]


@pytest.fixture
def mock_async_repository_get_list() -> AsyncRepository:
    """Simple fixture to provide an instance of the 'get_list' mock repostory."""

    return MockAsyncRepositoryGetList()


class MockAsyncRepositoryAdd(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository with '_add' method."""

    async def _add(self, aggregate: MockAggregate):
        """Simple _add implemention w/ msg log as a simple indicator."""

        logger.debug("_add func in 'MockAsyncRepositoryAdd'")


@pytest.fixture
def mock_async_repository_add() -> AsyncRepository:
    """Simple fixture to provide an instance of the 'add' mock repository."""

    return MockAsyncRepositoryAdd()


safe_name


class MockAsyncRepositoryAddThrowException(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository which throws exception on '_add'."""

    async def _add(self, aggregate: MockAggregate):
        """Raises an exception so we can ensure this method was invoked."""

        raise Exception


@pytest.fixture
def mock_async_repository_add_raise_exception() -> AsyncRepository:
    """Simple fixture to provide an instance of the '_add' w/ raise exception."""

    return MockAsyncRepositoryAddThrowException()


class MockAsyncRepositoryUpdate(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository with '_update' method."""

    async def _update(self, aggregate: MockAggregate):
        """Simple _update implemention w/ msg log as a simple indicator."""

        logger.debug("_update func in 'MockAsyncRepositoryUpdate'")


@pytest.fixture
def mock_async_repository_update() -> AsyncRepository:
    """Simple fixture to provide an instance of the 'update' repository."""

    return MockAsyncRepositoryUpdate()


class MockAsyncRepositoryUpdateRaiseException(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository with '_update' w/ raise exception."""

    async def _update(self, aggregate: MockAggregate):
        """Simple _update implementation w/ raised exception."""

        raise Exception


@pytest.fixture
def mock_async_repository_update_raise_exception() -> AsyncRepository:
    """Simple fixture to provide an instance of the 'update' repository."""

    return MockAsyncRepositoryUpdateRaiseException()


@pytest.mark.parametrize(
    argnames=["mock_repository"],
    argvalues=[
        (MockAsyncRepositoryGet(),),
        (MockAsyncRepositoryGetList(),),
        (MockAsyncRepositoryAdd(),),
        (MockAsyncRepositoryAddThrowException(),),
        (MockAsyncRepositoryUpdate(),),
        (MockAsyncRepositoryUpdateRaiseException(),),
    ],
)
def test_baseline_mock_async_repository(mock_repository: AsyncRepository):
    """Verifies given repo is conforms to expected invariants."""

    # Verify the repository correctly subclasses async repo
    assert issubclass(type(mock_repository), AsyncRepository)

    # Verify the repo has no 'seen' aggregates
    assert len(mock_repository.seen) == 0

    # Extend as needed if known invariants expand


async def test_async_repo_get_returns_correct_aggregate(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get returns expected MockAggregate."""

    # Get the aggregate with the given id
    agg = await mock_async_repository_get.get(pk=mock_aggregate.pk)
    # Assert the returned id matches the given id
    assert agg.pk == mock_aggregate.pk


async def test_async_repo_get_returns_none_when_not_found(
    mock_uuid: UUID,
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get returns 'None' when agg is not found."""

    # Get the aggregate with the given id
    agg = await mock_async_repository_get.get(pk=mock_uuid)
    # Assert the returned id matches the given id
    assert agg is None


async def test_async_repo_get_seen_is_correct(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get invokes seen response."""

    # Perform get to setup testcases
    agg = await mock_async_repository_get.get(
        pk=mock_aggregate.pk
    )  # id is optional on mock repo
    # Asserts AsyncRepository.get() adds the aggregate to the seen aggregates
    assert len(mock_async_repository_get.seen) == 1
    # Access the one aggregate, and confirm it's this one
    seen_agg = mock_async_repository_get.seen.pop()
    assert seen_agg.pk == agg.pk


async def test_async_repo_get_seen_is_correct_when_not_found(
    mock_uuid: UUID,
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get doesn't affect seen when agg not found."""

    # Perform get to setup testcases
    await mock_async_repository_get.get(pk=mock_uuid)
    # Asserts AsyncRepository.get() adds the aggregate to the seen aggregates
    assert len(mock_async_repository_get.seen) == 0


async def test_async_repo_get_list_returns_list(
    mock_async_repository_get_list: AsyncRepository,
):
    """Verifies AsyncRespository get_list baseline is correct."""

    aggregate_list = await mock_async_repository_get_list.get_list()

    assert isinstance(aggregate_list, list)
    # Mock repo implementation setup to return an arbitrary list
    assert len(aggregate_list) > 0
    # Assert the list is of aggregates
    for agg in aggregate_list:
        assert isinstance(agg, Aggregate)


async def test_async_repo_get_list_seen_count_is_correct(
    mock_async_repository_get_list: AsyncRepository,
):
    """Verifies AsyncRepository get_list invokes seen response for each aggregate."""

    # Invoke get_list to setup testcase
    aggregate_list = await mock_async_repository_get_list.get_list()
    # Test the amount of seen aggregates matches amount in aggregate_list
    assert len(mock_async_repository_get_list.seen) == len(aggregate_list)


async def test_async_repo_get_list_seen_aggregates_are_correct(
    mock_async_repository_get_list: AsyncRepository,
):
    """Verifies AsyncRepository get_list invokes seen response for each aggregate."""

    # Invoke get_list to setup testcase
    aggregate_list = await mock_async_repository_get_list.get_list()
    # Test each aggregate in list has been seen
    returned_agg_mapping = {agg.pk: True for agg in aggregate_list}
    for agg in mock_async_repository_get_list.seen:
        assert returned_agg_mapping[agg.pk]


async def test_mock_async_repo_add_simple_call(
    mock_aggregate: Aggregate,
    mock_async_repository_add: AsyncRepository,
):
    """Verifies AsyncRepository add is invoked w/o issue."""

    await mock_async_repository_add.add(mock_aggregate)


async def test_mock_async_repo_add_calls__add(
    mock_aggregate: Aggregate,
    mock_async_repository_add_raise_exception: AsyncRepository,
):
    """Verifies AsyncRepository add invokes _add."""

    with pytest.raises(Exception):
        # The mock repo has been setup to raise an exception within _add
        await mock_async_repository_add_raise_exception.add(mock_aggregate)


async def test_mock_async_repo_add_seen_is_correct(
    mock_aggregate: Aggregate,
    mock_async_repository_add: AsyncRepository,
):
    """Verifies AsyncRepository add invokes the seen response."""

    # Invoke add to setup testcase
    await mock_async_repository_add.add(mock_aggregate)
    # Test that only one agg has been seen, and it is the aggregate added
    assert len(mock_async_repository_add.seen) == 1
    assert mock_aggregate.pk == mock_async_repository_add.seen.pop().pk


async def test_mock_async_repo_update(
    mock_aggregate: Aggregate,
    mock_async_repository_update: AsyncRepository,
):
    """Verifies AsyncRepository update is invoked w/o issue."""

    await mock_async_repository_update.update(mock_aggregate)


async def test_mock_async_repo_update_raise_exception(
    mock_aggregate: Aggregate,
    mock_async_repository_update_raise_exception: AsyncRepository,
):
    """Verifies AsyncRepository update invokes _update."""

    with pytest.raises(Exception):
        # The mock repo has been setup to raise an exception within _update
        await mock_async_repository_update_raise_exception.update(mock_aggregate)


async def test_mock_async_repo_update_seen_is_correct(
    mock_aggregate: Aggregate,
    mock_async_repository_update: AsyncRepository,
):
    """Verifies AsyncRepository update invokes the seen response."""

    # Invoke update to setup testcase
    await mock_async_repository_update.update(mock_aggregate)
    # Test that seen has one agg in it
    assert len(mock_async_repository_update.seen) == 1
    # Ensure it matches the mock agg
    assert mock_aggregate.pk == mock_async_repository_update.seen.pop().pk


async def test_mock_async_repo_get_list_seen_is_correct(
    mock_aggregate: Aggregate,
    mock_async_repository_update: AsyncRepository,
):
    """Verifies AsyncRepository update invokes the seen response."""

    # Invoke update to setup testcase
    await mock_async_repository_update.update(mock_aggregate)
    # Test that seen has one agg in it
    assert len(mock_async_repository_update.seen) == 1
    # Ensure it matches the mock agg
    assert mock_aggregate.pk == mock_async_repository_update.seen.pop().pk


async def test_mock_async_repo_get_returns_correct_aggregate(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get returns expected MockAggregate."""

    # Get the aggregate with the given id
    agg = await mock_async_repository_get.get(pk=mock_aggregate.pk)
    # Assert the returned id matches the given id
    assert agg.pk == mock_aggregate.pk
