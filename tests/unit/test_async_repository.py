import pytest

from microservices.domain import Aggregate
from microservices.repository import AsyncRepository
from microservices.utils import get_logger, uuid4

logger = get_logger()


class MockAggregate(Aggregate):
    """..."""

    def __init__(self, id: str = None):
        self.id = id if id else str(uuid4())


@pytest.fixture
def mock_aggregate() -> MockAggregate:
    """..."""

    return MockAggregate()


class MockAsyncRepositoryGet(AsyncRepository[MockAggregate]):
    """..."""

    async def _get(self, id: str) -> MockAggregate | None:
        if id == "NOT_FOUND":
            return None

        return MockAggregate(id=id)


@pytest.fixture
def mock_async_repository_get() -> AsyncRepository:
    """Simple Fixture to provide an instance of the 'get' mock repository."""

    return MockAsyncRepositoryGet()


class MockAsyncRepositoryAdd(AsyncRepository[MockAggregate]):
    """..."""

    async def _add(self, aggregate: MockAggregate):
        logger.msg("_add func in 'MockAsyncRepository'")


@pytest.fixture
def mock_async_repository_add() -> AsyncRepository:
    """Simple fixture to provide an instance of the 'add' mock repository."""

    return MockAsyncRepositoryAdd()


class MockAsyncRepositoryAddThrowException(AsyncRepository[MockAggregate]):
    """Test implementation of AsyncRepository which throws exception on _add."""

    async def _add(self, aggregate: MockAggregate):
        """Raises an exception so we can ensure this method was invoked."""

        raise Exception


@pytest.fixture
def mock_async_repository_add_throw_exception() -> AsyncRepository:
    """Simple fixture to provide an instance of the '_add' w/ throw repository."""

    return MockAsyncRepositoryAddThrowException()


@pytest.mark.parametrize(
    argnames=["mock_repository"],
    argvalues=[
        (MockAsyncRepositoryGet(),),
        (MockAsyncRepositoryAdd(),),
    ],
)
def test_baseline_mock_async_repository(mock_repository: AsyncRepository):
    """Verifies given mock repo is a valid baseline."""

    # Verify the repository correctly subclasses async repo
    assert issubclass(type(mock_repository), AsyncRepository)

    # Verify the repo has no 'seen' aggregates
    assert len(mock_repository.seen) == 0


async def test_mock_async_repo_get_returns_correct_aggregate(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get returns expected MockAggregate."""

    # Get the aggregate with the given id
    agg = await mock_async_repository_get.get(id=mock_aggregate.id)
    # Assert the returned id matches the given id
    assert agg.id == mock_aggregate.id


async def test_mock_async_repo_get_returns_none_when_not_found(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get returns 'None' when agg is not found."""

    # Get the aggregate with the given id
    agg = await mock_async_repository_get.get(id="NOT_FOUND")
    # Assert the returned id matches the given id
    assert agg is None


async def test_mock_async_repo_get_seen_is_correct(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get invokes seen response."""

    # Perform get to setup testcases
    agg = await mock_async_repository_get.get(
        id=mock_aggregate.id
    )  # id is optional on mock repo
    # Asserts AsyncRepository.get() adds the aggregate to the seen aggregates
    assert len(mock_async_repository_get.seen) == 1
    # Access the one aggregate, and confirm it's this one
    seen_agg = mock_async_repository_get.seen.pop()
    assert seen_agg.id == agg.id


async def test_mock_async_repo_get_seen_is_correct_when_not_found(
    mock_aggregate: Aggregate,
    mock_async_repository_get: AsyncRepository,
):
    """Verifies AsyncRepository get doesn't affect seen when agg not found."""

    # Perform get to setup testcases
    await mock_async_repository_get.get(id="NOT_FOUND")
    # Asserts AsyncRepository.get() adds the aggregate to the seen aggregates
    assert len(mock_async_repository_get.seen) == 0


async def test_mock_async_repo_add_simple_call(
    mock_aggregate: Aggregate,
    mock_async_repository_add: AsyncRepository,
):
    """Verifies AsyncRepository add can be invoked w/o issue."""

    await mock_async_repository_add.add(mock_aggregate)


async def test_mock_async_repo_add_calls__add(
    mock_aggregate: Aggregate,
    mock_async_repository_add_throw_exception: AsyncRepository,
):
    """Verifies AsyncRepository add invokes _add."""

    with pytest.raises(Exception):
        # The mock repo has been setup to raise an
        await mock_async_repository_add_throw_exception.add(mock_aggregate)


async def test_mock_async_repo_add_seen_is_correct(
    mock_aggregate: Aggregate,
    mock_async_repository_add: AsyncRepository,
):
    """Verifies AsyncRepository add invokes the seen response."""

    # Invoke add to setup testcase
    await mock_async_repository_add.add(mock_aggregate)
    # Test that only one agg has been seen, and it is the aggregate added
    assert len(mock_async_repository_add.seen) == 1
    assert mock_aggregate.id == mock_async_repository_add.seen.pop().id
