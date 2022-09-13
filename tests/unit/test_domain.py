from uuid import UUID

import pytest

from microservices.domain import Aggregate, Entity, create_entity
from microservices.messages import Event, EventStream
from microservices.utils import get_logger, get_uuid
from tests.conftest import MockAggregate


class MockEntity(Entity):
    def __init__(self, id: UUID) -> None:
        self._id = id


@pytest.fixture
def mock_entity() -> MockEntity:
    return MockEntity(id=get_uuid())


class MockEntityNoID(Entity):
    def __init__(self, id: UUID) -> None:
        pass


@pytest.fixture
def mock_entity_no_id() -> MockEntityNoID:
    return MockEntityNoID(id=get_uuid())


class MockAggregateWithAttrs(Aggregate):
    """..."""

    def __init__(
        self,
        id: UUID,
        name: str,
        phone: str | None,
    ):
        """Simple test implementation to capture some attributes in aggregate."""

        self._id = id
        self._name = name
        self._phone = phone

        super().__init__()

    @classmethod
    def create(cls, id: UUID, name: str, phone: str | None = None):
        """Simple create method w/ b few attributes."""

        return create_entity(cls=cls, id=id, name=name, phone=phone)

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


def test_entity_with_no_id_raises_attr_error_on_access(
    mock_entity_no_id: Entity,
):
    """Verifies an incorrectly id-deficient mock entity raises the error."""

    with pytest.raises(AttributeError):
        mock_entity_no_id.id


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
    mock_aggregate: Aggregate,
    mock_a_event: Event,
):
    """Verifies adding a new event places event in events list."""

    assert len(mock_aggregate.events) == 0

    mock_aggregate.new_event(mock_a_event)

    assert len(mock_aggregate.events) == 1

    event_from_events = mock_aggregate.events.pop(0)
    assert event_from_events.stream == EventStream.MockA
    assert event_from_events is mock_a_event


def test_initialized_values_are_correct_immediately_after_init():
    """Verifies captured values on init are correct."""

    attrs = {"id": get_uuid(), "name": "Some Name", "phone": None}

    agg = MockAggregateWithAttrs.create(**attrs)

    assert attrs == agg.initialized_values


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

    assert attrs == agg.initialized_values


def test_changed_values_is_empty_when_no_changes(mock_aggregate_with_attrs: Aggregate):
    """Verifies changed values is empty when no changes are made."""

    assert mock_aggregate_with_attrs.changed_values == {}


def test_change_one_value_verify_changed_values_is_correct(
    mock_aggregate_with_attrs: MockAggregateWithAttrs,
):
    """..."""

    new_phone = "(111)111-1111"
    mock_aggregate_with_attrs.phone = new_phone

    assert mock_aggregate_with_attrs.changed_values == {"phone": new_phone}


def test_create_entity_uses_id_when_given():
    id = get_uuid()
    entity = create_entity(MockEntity, id=id)

    assert entity.id is not None
    assert entity.id.hex == id.hex


def test_create_entity_generates_id_when_not_given():
    entity = create_entity(MockEntity)

    assert entity.id is not None
    assert type(entity.id) == UUID


def test_create_entity_returns_correct_instance_type():
    """Verifies the entity returned is of the correct type."""

    entity = create_entity(MockEntity)

    assert type(entity) == MockEntity
