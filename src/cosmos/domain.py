from __future__ import annotations

from abc import ABC
from collections import namedtuple
from datetime import date, datetime as dt
from enum import Enum
from typing import Any, Dict, List, Protocol, Tuple
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from cosmos.utils import get_uuid


class DomainObject(ABC):
    def __init__(self, **kwargs):
        """Captures initial properties, and sets attributes on object.

        Implementations must pass all object properties through __init__.
        This allows the capture of the initial state of the object,
        and dynamically sets the object attributes.
        """

        for k, v in kwargs.items():
            setattr(self, k, v)

        def func(**kwargs):
            # Capture a snapshot of the values of this entity during initialization
            d = {}

            for attr, value in kwargs.items():
                if isinstance(value, DomainObject):
                    d[attr.strip("_")] = func(value._initial_properties)
                else:
                    d[attr.strip("_")] = value

            return d

        self._initial_properties = func(**kwargs)

    def to_dict(self):
        """Basic interpretation of self as a dict."""

        d={k: getattr(self, k) for k in self._initial_properties}
        print(d)
        return d
    
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
    _id: UUID
    created_at: date
    updated_at: date

    @staticmethod
    def create_entity(cls, **kwargs):
        """If id not passed, enhance kwargs to have it, otherwise validate."""

        uuid = kwargs.pop("id", None)

        if uuid is None:
            uuid = uuid4()

        elif type(uuid) != UUID:
            try:
                uuid = UUID(uuid)  # cast to UUID
            except (ValueError, TypeError):  
                uuid = uuid4()  # malformed id

        kwargs["_id"] = uuid

        return cls(
            created_at=dt.now(),
            updated_at=dt.now(),
            **kwargs,
        )

    def __eq__(self, other: object) -> bool:
        """Simple equality for all domain objects, based on ID."""

        if self == other:
            return True

        if type(other) == type(self):
            return self.id == other.id  # type: ignore

        return False

    @classmethod
    def create(self, *args, **kwargs):
        """Implemented Entities require the implementation of a create method."""

        raise NotImplementedError
    
    @property
    def initial_properties(self) -> Dict[str, Any]:
        """Simple public getter for initialized values."""

        return self._initial_properties
    
    @property
    def property_changes(self) -> Dict[str, Any]:
        """Calculates the diff between the init state and current state."""

        current_values = self.to_dict()

        return {
            k: v
            for k, v in current_values.items()
            # Only include k-v pairs where the value has changed
            if self.initial_properties[k] != current_values[k]
        }

    @property
    def id(self):
        return self._id

    @id.setter  # type: ignore
    def id(self, value):
        """Raise exception to prevent id from being changed during lifetime."""

        raise AttributeError("cannot set id on an existing entity instance")


class AggregateRoot(Entity, ABC):
    """Domain object to provide interface into domain."""

    def __init__(self, **kwargs):
        """Aggregate initialize function.

        Initializes an empty event list to capture events raised during the
        lifetime of this aggregate instance. Also captures a snapshot of the
        instance values upon initialization.

        NOTE: Aggregate implementations must accept an id parameter in __init__
        and assign the value to 'self._id'. They must also invoke the super __init__.
        """

        super().__init__(**kwargs)

        self._events = []

    @property
    def events(self) -> List[Event]:
        """Simple public getter for events."""

        return self._events

    def new_event(self, event: Event):
        """Append a new event to the internal list of events."""

        self._events.append(event)


class Consumer(AggregateRoot):
    """Aggregate for Event Stream Consumers."""

    name: str
    stream: str
    acked_id: str
    retroactive: bool

    @classmethod
    def create(  # type: ignore
        cls,
        *,
        id: UUID|None = None,
        stream: str,
        name: str,
        retroactive: bool = True,
    ) -> Consumer:
        """Standard create method for a Consumer aggregate."""

        new_consumer = Entity.create_entity(
            cls=cls,
            id=id,
            stream=stream,
            name=name,
            retroactive=retroactive,
            acked_id="0",  # deafult 'zero-value' of new consumer
        )

        # Tells mypy it's definitely a Consumer
        assert isinstance(new_consumer, Consumer)

        # EVAL: What else needs to occur here? Should we create a new consumer
        # event? Not sure if we would need that anytime, but possible good to
        # generate events as a rule?

        return new_consumer


class Message(BaseModel):
    """Base Message model/schema."""

    message_id: UUID = Field(default_factory=uuid4)

    class Config:
        use_enum_values = True

    @property
    def name(self) -> str:
        """..."""

        return type(self).__name__


class Event(Message):
    """Base Event of our domain model."""

    ...


class Command(Message):
    """Base Command message type created for extension."""

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


class EventConsume(Protocol):
    """Callback protocall for consuming events from the stream bus."""

    async def __call__(self, consumer: Consumer) -> Tuple[Event, str] | None:
        
        
        ...


ConsumerConfig = namedtuple("ConsumerConfig", "name handler retroactive")
DomainConsumerConfig = Dict[str, List[ConsumerConfig]]
