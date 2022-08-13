from typing import Generic, List, Set, TypeVar

from .domain import Aggregate

T = TypeVar("T", bound=Aggregate)


class AsyncRepository(Generic[T]):
    """ABC which enables an async aggregate persistence abstraction.

    The core responsbility of this class is track WHICH aggregate instances
    have passsed through the repository, via any of the available methods.
    The concrete repository implementation is responsible for persistence
    related implementation details.
    """

    def __init__(self):
        """Initializes set to track what aggregates have been seen."""

        self._seen: Set[T] = set()

    @property
    def seen(self) -> Set[T]:
        return self._seen

    async def get(self, id: str) -> T:
        """Call subclass _get implementation and note the aggregate as seen.

        :param: id -> the UUID of Aggregate to get
        """

        agg = await self._get(id=id)

        if agg:
            self._seen.add(agg)

        return agg

    async def _get(self, id: str) -> T:
        """Required for repository implementation to get an instance type 'T'."""

        raise NotImplementedError

    async def get_list(self, **kwargs) -> List[T]:
        """Call subclass _get_list implementation and note the aggregates as seen.

        :param: **kwargs -> any possible keyword arguments parameters used by
                _get_list implementation
        """

        agg_list = await self._get_list(**kwargs)

        # EVAL: Possible performance implication of checking self._seen
        # for large lists here? Likely unnecessary micro optimization at this time
        if agg_list:
            self._seen.update(agg_list)

        return agg_list

    async def _get_list(self, **kwargs) -> List[T]:
        """Required for repository implementation to get a list of type 'T'."""

        raise NotImplementedError

    async def add(self, aggregate: T):
        """Call subclass _add implementation and note the aggregate as seen.

        :param: aggregate -> an aggregate instance to be newly persisted
        """
        await self._add(aggregate)

        self._seen.add(aggregate)

    async def _add(self, aggregate: T):
        """Required for repository implementation to add an instance of type 'T'."""

        raise NotImplementedError

    async def update(self, aggregate: T):
        """Call subclass _update implementation and note the aggregate as seen.

        :param: aggregate -> an aggregate instance to update the values of
        """

        await self._update(aggregate)

        self._seen.add(aggregate)

    async def _update(self, aggregate: T):
        """Required for repository implementation to update an instances of type 'T'."""

        raise NotImplementedError
