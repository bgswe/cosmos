from typing import Type
from uuid import UUID

from cosmos.domain import AggregateRoot, Event


class AggregateRepository:
    """ABC which enables an aggregate persistence abstraction"""

    def __init__(self, singleton_config: dict):
        """Initializes set to track what aggregates have been seen"""

        self._seen = {}
        self._singleton_config = singleton_config

    @property
    def seen(self) -> list[AggregateRoot]:
        return list(self._seen.values())

    def reset(self):
        self._seen = {}

    def _mark_seen(self, aggregate: AggregateRoot):
        """Utility to add a given aggregate to the set of seen aggregates"""

        self._seen[aggregate.stream_id] = aggregate

    async def get(
        self, id: UUID, aggregate_root_class: Type[AggregateRoot]
    ) -> AggregateRoot | None:
        """Call subclass _get implementation and note the aggregate as seen

        :param: id -> the UUID of Aggregate to get
        """

        agg = await self._get(id=id, aggregate_root_class=aggregate_root_class)

        if agg is not None:
            self._mark_seen(aggregate=agg)

        return agg

    async def get_singleton(
        self,
        aggregate_root_class: Type[AggregateRoot],
    ) -> AggregateRoot | None:
        """Requests a singleton class, and if it's not present, instantiates it"""

        current_singleton_id = self._singleton_config.get(aggregate_root_class.__name__)

        agg = await self._get(
            id=current_singleton_id, aggregate_root_class=aggregate_root_class
        )

        if agg is None:
            agg = aggregate_root_class()
            agg.create(current_singleton_id)
            await self.save(aggregate=agg)

        self._mark_seen(aggregate=agg)

        return agg

    async def save(self, aggregate: AggregateRoot):
        """Call subclass _save implementation and note the aggregate as seen"""

        await self._save(aggregate)
        self._mark_seen(aggregate=aggregate)

    async def _get(
        self, id: UUID, aggregate_root_class: Type[AggregateRoot]
    ) -> AggregateRoot | None:
        """Required for repository implementation to get an AggregateRoot"""

        raise NotImplementedError

    async def _get_list(self, **kwargs) -> list[AggregateRoot]:
        """Required for repository implementation to get a list of AggregateRoots"""

        raise NotImplementedError

    async def _save(self, aggregate: AggregateRoot):
        """Required for repository implementation to persist an AggregateRoot"""

        raise NotImplementedError
