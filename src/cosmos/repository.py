from typing import Dict, List, Protocol, Tuple, Type
from uuid import UUID

from cosmos.domain import AggregateRoot, Event


class ReplaysAggregate(Protocol):
    """Interface for port into reconstituting an Aggregate given an event stream"""

    def replay(self, event_stream: List[Event]) -> AggregateRoot:
        """Replays the event stream and returns the resulting AggregateRoot"""

        pass


class HydratesEvent(Protocol):
    """Interface for port into reconstituting an Event given an event record"""

    def hydrate(
        self, event_record: List[Dict]
    ) -> Tuple[Type[AggregateRoot], List[Event]]:
        """Deserializes an event list and return a list of Event objects, and the AggregateRoot Type"""

        ...


class AggregateRepository:
    """ABC which enables an aggregate persistence abstraction"""

    def __init__(self):
        """Initializes set to track what aggregates have been seen"""

        self._seen = []

    @property
    def seen(self) -> List[AggregateRoot]:
        return self._seen

    def _mark_seen(self, aggregate: AggregateRoot):
        """Utility to add a given aggregate to the set of seen aggregates"""

        self._seen.append(aggregate)

    async def get(self, id: UUID) -> AggregateRoot | None:
        """Call subclass _get implementation and note the aggregate as seen

        :param: id -> the UUID of Aggregate to get
        """

        agg = await self._get(id=id)

        if agg:
            self._mark_seen(aggregate=agg)

        return agg

    async def get_list(self, **kwargs) -> List[AggregateRoot]:
        """Call subclass _get_list implementation and note the aggregates as seen.

        :param: **kwargs -> any possible keyword arguments parameters used by
                _get_list implementation
        """

        agg_list = await self._get_list(**kwargs)

        # EVAL: Possible performance implication of checking self._seen
        # for large lists here? Likely unnecessary micro optimization at this time
        if agg_list:
            for agg in agg_list:
                self._mark_seen(aggregate=agg)

        return agg_list

    async def save(self, aggregate: AggregateRoot):
        """Call subclass _save implementation and note the aggregate as seen"""

        await self._save(aggregate)

        self._mark_seen(aggregate=aggregate)

    async def _get(self, id: UUID) -> AggregateRoot | None:
        """Required for repository implementation to get an AggregateRoot"""

        raise NotImplementedError

    async def _get_list(self, **kwargs) -> List[AggregateRoot]:
        """Required for repository implementation to get a list of AggregateRoots"""

        raise NotImplementedError

    async def _save(self, aggregate: AggregateRoot):
        """Required for repository implementation to persist an AggregateRoot"""

        raise NotImplementedError


class AggregateEventStoreRepository(AggregateRepository):
    def __init__(
        self,
        replay_handler: ReplaysAggregate,  # TODO: revisit this param name
    ):
        """Initializes set to track what aggregates have been seen"""

        super().__init__()

        self._replay_handler = replay_handler


class AggregateReplay:
    event_hydrator: HydratesEvent

    def replay(self, event_records: List[Dict]):
        """..."""

        AggregateRootClass, event_stream = self.event_hydrator.hydrate(
            event_records=event_records,
        )

        aggregate_root = AggregateRootClass()

        for event in event_stream:
            aggregate_root.mutate(event=event)

        return aggregate_root
