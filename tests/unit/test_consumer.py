import pytest

from cosmos.domain import Consumer
from cosmos.utils import get_uuid


@pytest.fixture
def mock_consumer() -> Consumer:
    return Consumer.create(
        stream="MockA",
        name="Some Name",
    )


def test_consumer_create_calls_has_entity_meta_fields():
    """Verifies artifacts created in super init exists on consumer."""

    c = Consumer.create(
        id=get_uuid(),
        stream="MockA",
        name="SomeConsumer",
        retroactive=False,
    )

    # These are the two attrs added in Aggregate.__init__
    assert hasattr(c, "_events")
    assert hasattr(c, "_initial_properties")


def test_new_consumer_has_acked_id_zero(mock_consumer: Consumer):
    """Verifies that a consumer beings with '0' as ID when no messages acked."""

    assert mock_consumer.acked_id == "0"
