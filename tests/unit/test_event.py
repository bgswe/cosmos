from uuid import UUID

from microservices.events import Event


def test_event_assigns_uuid_if_not_given(
    mock_a_event: Event,
):
    """Verifies the root Event.id is populated with a valid uuid."""

    assert mock_a_event.id is not None

    try:
        UUID(mock_a_event.id)
    except:  # noqa
        assert False  # not a valid uuid


def test_event_domain_property_returns_correct_domain(
    mock_a_event: Event,
):
    """Verifies the root Event.domain property returns correctly."""

    assert mock_a_event.domain == "domain"
