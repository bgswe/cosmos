from __future__ import annotations

from abc import ABC
from typing import Any, Dict, List, Type, Union

from pydantic import BaseModel, constr

from microservices.events import Event, EventStream

from .utils import uuid4


def uuid_constr() -> Type[str]:
    """Returns configured constr from pydantic with UUIDv4 constraints."""

    return constr(
        min_length=36,
        max_length=36,
    )


class Command(BaseModel):
    """Base Command of our domain model.

    Includes the client_id of the invoker of a given command.
    """

    client_id: uuid_constr()  # type: ignore


class Entity(ABC):
    """Base Entity of our domain model."""

    id: str

    def __init__(self):
        """Entity initialize func

        Sets id to new UUID if not specified by children __init__ functions, and
        captures the values of the Entities private/public attrs upon initialization.
        """

        def func(e: Entity):
            # Capture a snapshot of the values of this entity during initialization
            initialized_values = {}

            for attr, value in e.__dict__.items():
                if isinstance(value, Entity):
                    initialized_values[attr.strip("_")] = func(value)
                elif attr not in ["_events", "_initialized_values"]:
                    initialized_values[attr.strip("_")] = value

            return initialized_values

        self._initialized_values = func(self)

    @property
    def initialized_values(self) -> Dict[str, Any]:
        return self._initialized_values

    @property
    def changed_values(self) -> Dict[str, Any]:
        changed_values = {}

        for attr, value in self.__dict__.items():
            if attr not in ["_events", "_initialized_values"]:
                key = attr.strip("_")

                if isinstance(value, Entity):
                    entity_changed_values = value.changed_values

                    if len(entity_changed_values.keys()):
                        changed_values[key] = entity_changed_values
                else:
                    if value != self.initialized_values[key]:
                        changed_values[key] = value

        return changed_values


class Aggregate(Entity, ABC):
    """Aggregate to serve as an entry point into domain."""

    def __init__(self):
        """Aggregate initialize function.

        Creates new list to capture new events during this lifespan of an
        aggregate.
        """
        super().__init__()

        self._events: List[Event] = []

    @property
    def has_events(self) -> bool:
        return len(self._events) > 0

    def new_event(self, event: Event):
        self._events.append(event)

    def get_events(self) -> List[Event]:
        return self._events


def create_entity(
    cls: Type[Entity],
    **init_kwargs,
) -> Union[Entity, Aggregate]:
    """Encapsulates housekeeping tasks of entity creation.

    Takes a subclass of Entity and a dictionary of kwargs which
    is used as arguments to the subclass' __init__.
    """
    # Auto-assign UUID if not provided by entity creator
    if init_kwargs.get("id", None) is None:
        init_kwargs = {**init_kwargs, "id": uuid4()}

    return cls(**init_kwargs)


class Consumer(Aggregate):
    """Aggregate for Event Stream Consumers."""

    def __init__(
        self,
        id: str,
        stream: EventStream,
        name: str,
        acked_id: str,
        retroactive: bool,
    ):
        self.id = id
        self.stream = stream
        self.name = name
        self.acked_id = acked_id
        self.retroactive = retroactive

        super().__init__()

    @classmethod
    def create(
        cls,
        stream: EventStream,
        name: str,
        id: str = None,
        retroactive: bool = True,
    ) -> Consumer:
        new_consumer = create_entity(
            cls=cls,
            id=id,
            stream=stream,
            name=name,
            acked_id="0",
            retroactive=retroactive,
        )

        # Tells mypy it's definitely a Consumer
        assert isinstance(new_consumer, Consumer)

        # TODO: ...

        return new_consumer
