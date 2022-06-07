from abc import ABC

from pydantic import BaseModel, constr

from typing import Any, Dict, List, Type
from .utils import uuid4


class Event:
    """Base Event of our domain model."""

    pass


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
    client_id: uuid_constr()


class Entity(ABC):
    """Base Entity of our domain model."""

    def __init__(self):
        """Entity initialize func

        Sets id to new UUID if not specified by children __init__ functions, and
        captures the values of the Entities private/public attrs upon initialization.
        """

        # Generate ID if not specified in __init__
        if self._id is None:
            self._id = uuid4()

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
    def id(self):
        return self._id

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
