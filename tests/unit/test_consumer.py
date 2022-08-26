import pytest

from microservices.domain import Consumer
from microservices.messages import EventStream


@pytest.fixture
def mock_consumer() -> Consumer:
    return Consumer.create(
        stream=EventStream.MockA,
        name="Some Name",
    )
