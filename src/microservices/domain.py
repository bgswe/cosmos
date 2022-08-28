from __future__ import annotations

from abc import ABC
from typing import Any, Dict, List, Type, Union
from uuid import UUID

from microservices.events import Event, EventStream
from microservices.utils import get_uuid


class Entity(ABC):
    """Base Entity of our domain model."""

    _id: UUID  # all entities will have id

    @property
    def id(self) -> UUID:
        """Simple public getter for id."""

        return self._id

    @id.setter  # type: ignore
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
    **init_kwargs,
) -> Union[Entity, Aggregate]:
    """Encapsulates housekeeping tasks of entity creation.

    Creating a new instance of an Entity doesn't only arise
    from creating a new Entity identity, so this function acts as
    a wrapper for the creation process. A new UUID is generated
    if not supplied via __init__ kwargs.
    """

    # Auto-assign UUID if not provided by entity creator
    if init_kwargs.get("id", None) is None:
        init_kwargs = {**init_kwargs, "id": get_uuid()}

    # EVAL: What else could go here?

    return cls(**init_kwargs)


class Consumer(Aggregate):
    """Aggregate for Event Stream Consumers."""

    def __init__(
        self,
        id: UUID,
        stream: EventStream,
        name: str,
        acked_id: str,
        retroactive: bool,
    ):
        self._id = id
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
        id: UUID = None,
        retroactive: bool = True,
    ) -> Consumer:
        """Standard create method for a Consumer aggregate."""

        new_consumer = create_entity(
            cls=cls,
            id=id,
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
    def acked_id(self) -> str:
        """Simple getter for '_acked_id'."""

        return self._acked_id
