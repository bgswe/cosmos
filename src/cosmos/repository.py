from typing import Type
from uuid import UUID

from cosmos.domain import AggregateRoot, Event


class AggregateRepository:
    """ABC which enables an aggregate persistence abstraction"""

    def __init__(self):
        """Initializes set to track what aggregates have been seen"""

        self._seen = {}

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

        if agg:
            self._mark_seen(aggregate=agg)

        return agg

    async def get_list(self, **kwargs) -> list[AggregateRoot]:
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
