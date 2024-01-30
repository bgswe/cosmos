from __future__ import annotations

from abc import ABC
from datetime import datetime as dt
from enum import StrEnum, auto
from typing import Any, Dict, List
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


# class DomainObject(ABC):
#     def __init__(self, **kwargs):
#         """Sets the key value pairs as attributes"""

#         self._initialized = False

#         for k, v in kwargs.items():
#             setattr(self, k, v)

#         self._initialized = True


# class ValueObject(DomainObject):
#     def __eq__(self, other: Any) -> bool:
#         if not other:  # other is falsey
#             return False

#         if self == other:  # same object in memory
#             return True

#         if not isinstance(other, ValueObject):  # other not value object
#             return False

#         # compare shallow equality for property dicts
#         return self.to_dict() == other.to_dict()


class Entity(ABC):
    def __init__(self):
        """..."""

        self._initialized = False
        self._changed = False
        self._changed_attrs: Dict[str, Any] = {}

    def to_dict(self):
        """Basic interpretation of the instance as a dict"""

        _vars = vars(self)

        return {k: v for k, v in _vars.items() if not k.startswith("_")}

    def _initialize(self, **attrs):
        """..."""

        uuid = attrs.pop("id", None)

        if uuid is None:
            uuid = uuid4()

        attrs["id"] = uuid

        for attr, value in attrs.items():
            setattr(self, attr, value)

        self.initialized = True

    def __repr__(self):
        """Use dictionary representation as the default"""

        return str(self.to_dict())

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

    def _reset_changed(self):
        self._changed = False
        self._changed_attrs = {}


class AggregateRoot(Entity, ABC):
    """Domain object to provide interface into domain."""

    def __init__(self):
        """..."""

        super().__init__()

        self._events = []
        self._version = 0  # appears this will match version when loaded from repo

    @classmethod
    def replay(cls, events: List[Event]):
        instance = cls()

        for event in events:
            instance.mutate(event=event)

        return instance

    def mutate(self, event: Event):
        """Each AggregateRoot needs to define a _mutate method

        This method will decide how each event will change the
        aggregate's state
        """

        self._events = [*self._events, event]
        self._mutate(event=event)

    def _mutate(self, event: Event):
        raise NotImplementedError

    def _reset_internals(self):
        """Will reset events, changes, etc.

        This is useful when AggregateRoot changes to this point are of no concern,
        e.g. during replay, we want to reset this state before moving further with
        returning the AR for use in business logic.
        """

        self._events = []
        self._reset_changed()

    @property
    def events(self) -> List[Event]:
        """Simple public getter for events"""

        return self._events

    @property
    def name(self) -> str:
        """..."""

        return type(self).__name__


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


class DomainEvent(Event):
    """Event arising from the Domain"""

    stream_id: UUID


class SystemEvent(Event):
    """Event arising in abstraction levels above the Domain

    NOTE: Alternatively this can be a 'ServiceEvent'?
    """

    ...


class Command(Message):
    """Base Command message type created for extension"""

    ...


class CommandCompletionStatus(StrEnum):
    SUCCESS = auto()
    FAILURE = auto()


class CommandComplete(SystemEvent):
    """This is an event which is emitted upon completion of command handling"""

    timestamp: dt
    command_name: str
    command_id: UUID
    client_id: UUID | None = None
    status: CommandCompletionStatus


class AuthenticatedCommand(Message):
    """Command message which requires the invokee to be authenticated"""

    client_id: UUID  # client invoking command
