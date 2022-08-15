import pytest

from microservices.domain import Aggregate
from microservices.repository import AsyncRepository
from microservices.utils import uuid4


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


@pytest.fixture
def mock_async_repository() -> AsyncRepository:
    return MockAsyncRepository()
