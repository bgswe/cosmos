from __future__ import annotations

from abc import ABC
from datetime import datetime as dt
from enum import StrEnum, auto
from typing import Any, Dict, List, Protocol
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


class DomainObject(ABC):
    def __init__(self, **kwargs):
        """Sets the key value pairs as attributes"""

        self._initialized = False

        for k, v in kwargs.items():
            setattr(self, k, v)

        self._initialized = True

    def to_dict(self):
        """Basic interpretation of self as a dict."""

        _vars = vars(self)

        return {k: v for k, v in _vars.items() if not k.startswith("_")}

    def __repr__(self):
        """Use dictionary representation as the default."""

        return str(self.to_dict())


class ValueObject(DomainObject):
    def __eq__(self, other: Any) -> bool:
        if not other:  # other is falsey
            return False

        if self == other:  # same object in memory
            return True

        if not isinstance(other, ValueObject):  # other not value object
            return False

        # compare shallow equality for property dicts
        return self._initial_properties == other._initial_properties


class Entity(DomainObject, ABC):
    def __init__(self, **kwargs):
        uuid = kwargs.pop("id", None)

        if uuid is None:
            uuid = uuid4()

        elif type(uuid) != UUID:
            try:
                uuid = UUID(uuid)  # cast to UUID
            except (ValueError, TypeError):
                uuid = uuid4()  # malformed id

        kwargs["id"] = uuid

        super().__init__(**kwargs)

        self._changed = False
        self._changed_attrs: Dict[str, Any] = {}

    def __eq__(self, other: object) -> bool:
        """Simple equality for all domain objects, based on ID"""

        if self == other:
            return True

        if type(other) == type(self):
            return self.id == other.id  # type: ignore

        return False

    def __setattr__(self, attr: str, value: Any) -> None:
        """Override setattr to capture when and how entity attributes change"""

        if not attr.startswith("_") and self._initialized:
            self._changed = True
            self._changed_attrs[attr] = value

        return super().__setattr__(attr, value)


class AggregateRoot(Entity, ABC):
    """Domain object to provide interface into domain."""

    def __init__(self, **kwargs):
        """Aggregate initialize function

        Initializes an empty event list to capture events raised during the
        lifetime of this aggregate instance.
        """

        super().__init__(**kwargs)

        self._events = []
        self._version = 0

        # TODO: emit domain event -> CreatedAggregate

    @property
    def events(self) -> List[Event]:
        """Simple public getter for events"""

        return self._events

    @property
    def has_events(self) -> bool:
        """Simple implementation test of whether the aggregate has produced events"""

        return len(self._events) > 0

    def new_event(self, event: Event):
        """Add new event to event list"""

        self._events = [*self._events, event]


class Message(BaseModel):
    """Base Message model/schema"""

    message_id: UUID = Field(default_factory=uuid4)

    model_config = ConfigDict(use_enum_values=True)

    @property
    def name(self) -> str:
        """..."""

        return type(self).__name__


class Event(Message):
    """Base Event of our domain model"""

    ...


class Command(Message):
    """Base Command message type created for extension"""

    ...


class CommandCompletionStatus(StrEnum):
    SUCCESS = auto()
    FAILURE = auto()


class CommandComplete(Event):
    """This is an event which is emitted upon completion of command handling"""

    timestamp: dt
    command_name: str
    command_id: UUID
    status: CommandCompletionStatus


class AuthenticatedCommand(Message):
    """Command message which requires the invokee to be authenticated"""

    client_id: UUID  # client invoking command


class EventPublish(Protocol):
    """Callback protocall for publishing an event to the stream bus."""

    def __call__(self, event: Event):
        """Publishes internal event to external event stream.

        TODO: It's likely that this is not a sufficient signature.
        If not, then it would need to raise an expection on publication
        failure.
        """

        ...
