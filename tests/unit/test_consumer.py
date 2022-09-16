import pytest

from microservices.domain import Consumer, EventStream
from microservices.utils import get_uuid


@pytest.fixture
def mock_consumer() -> Consumer:
    return Consumer.create(
        stream=EventStream.MockA,
        name="Some Name",
    )


def test_consumer_init_calls_super_init():
    """Verifies artifacts created in super init exists on consumer."""

    c = Consumer(
        id=get_uuid(),
        stream=EventStream.MockA,
        name="SomeConsumer",
        acked_id="0",
        retroactive=False,
    )

    # These are the two attrs added in Aggregate.__init__
    assert hasattr(c, "_events")
    assert hasattr(c, "_initialized_values")


def test_new_consumer_has_acked_id_zero(mock_consumer: Consumer):
    """Verifies that a consumer beings with '0' as ID when no messages acked."""

    assert mock_consumer.acked_id == "0"
