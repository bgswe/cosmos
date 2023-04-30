from uuid import UUID

from cosmos.domain import Event


def test_event_assigns_uuid_if_not_given(
    mock_event_a: Event,
):
    """Verifies the root Event.id is populated with a valid uuid."""

    assert mock_event_a.message_id is not None
    assert type(mock_event_a.message_id) == UUID
