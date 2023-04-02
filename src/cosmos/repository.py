from typing import Generic, List, TypeVar
from uuid import UUID

from cosmos.domain import AggregateRoot

T = TypeVar("T", bound=AggregateRoot)


class AsyncRepository(Generic[T]):
    """ABC which enables an async aggregate persistence abstraction.

    The core responsbility of this class is track WHICH aggregate instances
    have passsed through the repository, via any of the available methods.
    The concrete repository implementation is responsible for persistence
    related implementation details.
    """

    def __init__(self):
        """Initializes set to track what aggregates have been seen."""

        self._seen: List[T] = []

    @property
    def seen(self) -> List[T]:
        return self._seen

    def _mark_seen(self, aggregate: T):
        """Utility to add a given aggregate to the set of seen aggregates."""

        self._seen.append(aggregate)

    async def get(self, pk: UUID) -> T:
        """Call subclass _get implementation and note the aggregate as seen.

        :param: id -> the UUID of Aggregate to get
        """

        agg = await self._get(pk=pk)

        if agg:
            self._mark_seen(aggregate=agg)

        return agg

    async def get_list(self, **kwargs) -> List[T]:
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

    async def add(self, aggregate: T):
        """Call subclass _add implementation and note the aggregate as seen.

        :param: aggregate -> an aggregate instance to be newly persisted
        """

        await self._add(aggregate)

        self._mark_seen(aggregate=aggregate)

    async def update(self, aggregate: T):
        """Call subclass _update implementation and note the aggregate as seen.

        :param: aggregate -> an aggregate instance to update the values of
        """

        await self._update(aggregate)

        self._mark_seen(aggregate=aggregate)

    async def _get(self, pk: UUID) -> T:
        """Required for repository implementation to get an instance type 'T'."""

        raise NotImplementedError

    async def _get_list(self, **kwargs) -> List[T]:
        """Required for repository implementation to get a list of type 'T'."""

        raise NotImplementedError

    async def _add(self, aggregate: T):
        """Required for repository implementation to add an instance of type 'T'."""

        raise NotImplementedError

    async def _update(self, aggregate: T):
        """Required for repository implementation to update an instances of type 'T'."""

        raise NotImplementedError
