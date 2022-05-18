from abc import ABC
from typing import List
from uuid import uuid4


class Event:
    pass


class Command:
    pass


class Entity(ABC):
    """Base Entity of our domain model."""

    def __init__(self):
        """Entity initialize func

        Sets id to new UUID if not specified by children __init__ functions.
        Creates flag to mark the entity as new or not (previously persisted).
        """
        self.new = False

        if self.id is None:
            self.id = str(uuid4())
            self.new = True


class Aggregate(Entity, ABC):
    """Aggregate to serve as an entry point into domain."""

    def __init__(self):
        """Aggregate initialize func

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
