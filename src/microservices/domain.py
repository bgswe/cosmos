from __future__ import annotations

from abc import ABC
from collections import namedtuple
from enum import Enum
from typing import Any, ClassVar, Dict, List, Protocol, Tuple, Type
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from microservices.utils import get_uuid

PK = UUID | Enum | str | int | Tuple[UUID | str | int]


class Entity(ABC):
    """Base Entity of our domain model."""

    _pk: PK  # all entities will have a primary key

    @property
    def pk(self) -> PK:
        """Simple public getter for the primary key."""

        return self._pk

    @pk.setter  # type: ignore
    def raise_exception(self):
        """Raise exception to prevent id from being changed during lifetime."""

        raise AttributeError("cannot set id on an existing entity instance")


class Aggregate(Entity, ABC):
    """Class to serve as an entry point into domain."""

    meta_attrs = ["_events", "_initialized_values"]

    def __init__(self):
        """Aggregate initialize function.

        Initializes an empty event list to capture events raised during the
        lifetime of this aggregate instance. Also captures a snapshot of the
        instance values upon initialization.

        NOTE: Aggregate implementations must accept and id parameter in __init__
        and assign the value to 'self._id'. They must also invoke the super __init__.
        """

        self._events: List[Event] = []
        self._initialized_values = self.to_dict()

    @property
    def events(self) -> List[Event]:
        """Simple public getter for events."""

        return self._events

    @property
    def initialized_values(self) -> Dict[str, Any]:
        """Simple public getter for initialized values."""

        return self._initialized_values

    @property
    def changed_values(self) -> Dict[str, Any]:
        """Calculates the diff between the init state and current state."""

        current_values = self.to_dict()

        return {
            k: v
            for k, v in current_values.items()
            # Only include k-v pairs where the value has changed
            if self._initialized_values[k] != current_values[k]
        }

    def new_event(self, event: Event):
        """Append a new event to the internal list of events."""

        self._events.append(event)

    def to_dict(self) -> Dict:
        """Serialize aggregate as dict."""

        def func(e: Entity):
            # Capture a snapshot of the values of this entity during initialization
            d = {}

            for attr, value in e.__dict__.items():
                if isinstance(value, Entity):
                    d[attr.strip("_")] = func(value)
                # This is essentially the list of 'Meta' attributes
                elif attr not in Aggregate.meta_attrs:
                    d[attr.strip("_")] = value

            return d

        return func(self)


def create_entity(
    cls: Type[Entity],
    has_uuid_pk: bool = True,
    **init_kwargs,
) -> Entity:
    """Encapsulates housekeeping tasks of entity creation.

    Creating a new instance of an Entity doesn't only arise
    from creating a new Entity identity, so this function acts as
    a wrapper for the creation process. A new UUID is generated
    if not supplied via __init__ kwargs.
    """

    # Auto-assign UUID if PK not provided by entity creator
    if has_uuid_pk and init_kwargs.get("pk", None) is None:
        init_kwargs = {**init_kwargs, "pk": get_uuid()}

    # EVAL: What else could go here?

    return cls(**init_kwargs)


class Consumer(Aggregate):
    """Aggregate for Event Stream Consumers."""

    def __init__(
        self,
        pk: PK,
        stream: EventStream,
        name: str,
        acked_id: str,
        retroactive: bool,
    ):
        self._pk = pk
        self._stream = stream
        self._name = name
        self._acked_id = acked_id
        self._retroactive = retroactive

        super().__init__()

    @classmethod
    def create(
        cls,
        stream: EventStream,
        name: str,
        pk: PK = None,
        retroactive: bool = True,
    ) -> Consumer:
        """Standard create method for a Consumer aggregate."""

        new_consumer = create_entity(
            cls=cls,
            pk=pk,
            stream=stream,
            name=name,
            acked_id="0",  # deafult 'zero-value' of new consumer
            retroactive=retroactive,
        )

        # Tells mypy it's definitely a Consumer
        assert isinstance(new_consumer, Consumer)

        # EVAL: What else needs to occur here? Should we create a new consumer
        # event? Not sure if we would need that anytime, but possible good to
        # generate events as a rule?

        return new_consumer

    @property
    def stream(self) -> EventStream:
        """Simple gett for the Consumer's event stream."""

        return self._stream

    @property
    def retroactive(self) -> bool:
        """Simple getter for the whether the consumer is retroactive or not."""

        return self._retroactive

    @property
    def name(self) -> str:
        """Simple getter for the Consumer's name."""

        return self._name

    @property
    def acked_id(self) -> str:
        """Simple getter for the acknowledged message ID."""

        return self._acked_id

    @acked_id.setter
    def acked_id(self, message_id: str):
        """Simple setter for the acknowledged message."""

        self._acked_id = message_id


class Domain(Enum):
    """The registered domains of the system."""

    Registration = "registration"
    Inspection = "inspection"

    # NOTE: Just for test
    Test = "test"


class EventStream(Enum):
    """The registered event streams in the system."""

    AccountRegistered = f"{Domain.Registration.value}.account_registered"
    AccountUserRegistered = f"{Domain.Registration.value}.account_user_registered"

    InspectionNew = f"{Domain.Inspection.value}.new"
    InspectionUpdated = f"{Domain.Inspection.value}.updated"
    InspectionCancelled = f"{Domain.Inspection.value}.cancelled"
    InspectionCancellationInvalid = f"{Domain.Inspection.value}.cancellation_invalid"

    # NOTE: These exist for testing purposes only
    # TODO: Try to figure out another way to have mock streams, w/o
    # changing source. Tried subclassing EventStream, nogo. :(
    MockA = "domain.stream_a"
    MockB = "domain.stream_b"
    MockC = "domain.stream_c"


class Message(BaseModel):
    """Base Message model/schema."""

    message_id: UUID = Field(default_factory=uuid4)

    class Config:
        use_enum_values = True


class Event(Message):
    """Base Event of our domain model."""

    # Each event has an associated named stream, essentially a type
    stream: ClassVar[EventStream]

    @property
    def domain(self) -> str:
        return self.stream.value.split(".")[0]


class EventPublish(Protocol):
    """Callback protocall for publishing an event to the stream bus."""

    def __call__(self, event: Event):
        """Publishes internal event to external event stream.

        TODO: It's likely that this is not a sufficient signature.
        If not, then it would need to raise an expection on publication
        failure.
        """

        ...


ConsumerConfig = namedtuple("ConsumerConfig", "name handler retroactive")
DomainConsumerConfig = Dict[EventStream, List[ConsumerConfig]]


class EventConsume(Protocol):
    """Callback protocall for consuming events from the stream bus."""

    async def __call__(self, consumer: Consumer) -> Tuple[Event, str] | None:
        ...


class Command(Message):
    """Base Command message type created for extension."""

    client_id: UUID  # client invoking command
