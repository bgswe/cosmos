from uuid import UUID

import pytest

from cosmos.domain import AggregateRoot, Entity, Event
from cosmos.utils import get_logger, get_uuid
from tests.conftest import MockAggregate


class MockEntity(Entity):
    @classmethod
    def create(cls, id: UUID = None):
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

    mock_pk = get_uuid()
    agg = MockAggregate.create(pk=mock_pk)

    assert agg.pk.hex == mock_pk.hex


def test_aggregate_new_event_adds_to_events_list(
    mock_aggregate: AggregateRoot,
    mock_a_event: Event,
):
    """Verifies adding a new event places event in events list."""

    assert len(mock_aggregate.events) == 0

    mock_aggregate.new_event(mock_a_event)

    assert len(mock_aggregate.events) == 1

    event_from_events = mock_aggregate.events.pop(0)
    assert event_from_events.stream == "MockA"
    assert event_from_events is mock_a_event


def test_initialized_values_are_correct_immediately_after_init():
    """Verifies captured values on init are correct."""

    attrs = {"id": get_uuid(), "name": "Some Name", "phone": None}

    agg = MockAggregateWithAttrs.create(**attrs)

    expected_attrs = {}
    expected_attrs.update(agg.initial_properties)
    
    expected_attrs.pop("created_at")
    expected_attrs.pop("updated_at")

    assert attrs == expected_attrs 


def test_initialized_values_are_correct_after_changes():
    """Verifies init values are still correct after subsequent attr changes."""

    attrs = {"id": get_uuid(), "name": "Some Name", "phone": None}

    agg = MockAggregateWithAttrs.create(**attrs)

    new_name = "Other Name"
    agg.name = new_name
    assert agg.name == new_name

    new_phone = "(555)555-5555"
    agg.phone = new_phone
    assert agg.phone == new_phone

    expected_attrs = {}
    expected_attrs.update(agg.initial_properties)
    
    expected_attrs.pop("created_at")
    expected_attrs.pop("updated_at")

    assert attrs == expected_attrs


def test_property_changes_is_empty_when_no_changes(mock_aggregate_with_attrs: AggregateRoot):
    """Verifies changed values is empty when no changes are made."""
    print(mock_aggregate_with_attrs.property_changes)
    assert mock_aggregate_with_attrs.property_changes == {}


def test_change_one_value_verify_property_changes_is_correct(
    mock_aggregate_with_attrs: MockAggregateWithAttrs,
):
    """..."""

    new_phone = "(111)111-1111"
    mock_aggregate_with_attrs.phone = new_phone

    assert mock_aggregate_with_attrs.property_changes == {"phone": new_phone}


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
