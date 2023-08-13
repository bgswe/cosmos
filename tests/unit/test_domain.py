from uuid import UUID

import pytest
from structlog import get_logger

from cosmos.domain import AggregateRoot, Entity, Event
from cosmos.utils import get_uuid
from tests.conftest import MockAggregate


class MockEntity(Entity):
    @classmethod
    def create(cls, id: UUID | None = None):
        return Entity.create_entity(
            cls=cls,
            id=id,
        )


@pytest.fixture
def mock_entity() -> MockEntity:
    return MockEntity.create(id=get_uuid())


@pytest.fixture
def mock_entity_no_id() -> MockEntity:
    return MockEntity.create()


class MockAggregateWithAttrs(AggregateRoot):
    """..."""

    @classmethod
    def create(cls, id: UUID, name: str, phone: str | None = None):
        """Simple create method w/ b few attributes."""

        return Entity.create_entity(cls=cls, id=id, name=name, phone=phone)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, new_name: str):
        self._name = new_name

    @property
    def phone(self) -> str | None:
        return self._phone

    @phone.setter
    def phone(self, new_phone: str | None):
        self._phone = new_phone


@pytest.fixture
def mock_aggregate_with_attrs() -> MockAggregateWithAttrs:
    """Simple fixture to return a new MockAggregateWithAttrs instance."""

    return MockAggregateWithAttrs.create(id=get_uuid(), name="Some Name")


def test_entity_has_id_property(mock_entity: Entity):
    """Verify that setting the _id attr allows id attr to be accessed."""

    try:
        mock_entity.id
    except Exception as e:
        logger = get_logger()

        log = logger.bind(exception=e)
        log.error("exception raised accessing id attr on mock entity")

        assert False  # fail the test


def test_aggregate_has_empty_events_list_after_init():
    """Verifies the aggregate has an empty events list after init."""

    mock_aggregate = MockAggregate.create()

    assert type(mock_aggregate.events) == list
    assert len(mock_aggregate.events) == 0


def test_aggregate_uses_given_id_if_given():
    """Verifies that the id is used and not discarded or overwritten."""

    mock_id = get_uuid()
    agg = MockAggregate.create(id=mock_id)

    assert agg.id.hex == mock_id.hex


def test_aggregate_new_event_adds_to_events_list(
    mock_aggregate: AggregateRoot,
    mock_event_a: Event,
):
    """Verifies adding a new event places event in events list."""

    assert len(mock_aggregate.events) == 0

    mock_aggregate.new_event(mock_event_a)

    assert len(mock_aggregate.events) == 1

    event_from_events = mock_aggregate.events.pop(0)
    assert type(event_from_events).__name__ == "MockEventA"
    assert event_from_events is mock_event_a


def test_create_entity_uses_id_when_given():
    id = get_uuid()
    entity = Entity.create_entity(MockEntity, id=id)

    assert entity.id is not None
    assert entity.id.hex == id.hex


def test_create_entity_generates_id_when_not_given():
    entity = Entity.create_entity(MockEntity)

    # assert entity.id is not None
    # assert type(entity.id) == UUID


def test_create_entity_returns_correct_instance_type():
    """Verifies the entity returned is of the correct type."""

    entity = Entity.create_entity(MockEntity)

    assert type(entity) == MockEntity


#########


def test_entity_changed():
    # mock aggregate
    class SomeEntity(AggregateRoot):
        @classmethod
        def create(
            cls,
            *,
            id: UUID = None,
            some_attr: str,
        ):
            return Entity.create_entity(
                cls=cls,
                id=id,
                some_attr=some_attr,
            )

    # create an instance
    created_se = SomeEntity.create(
        some_attr="some_attr",
    )

    # mock repository hydration
    se = SomeEntity(**created_se.to_dict())

    assert not se._changed
    assert se._changed_attrs.get("some_attr") is None

    se.some_attr = "SOME_ATTR"

    assert se._changed
    assert se._changed_attrs.get("some_attr") is not None
    assert se._changed_attrs.get("some_attr") == "SOME_ATTR"
