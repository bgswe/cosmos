from abc import abstractmethod
from typing import Generic, List, Set, TypeVar, Union

from .domain import Aggregate

T = TypeVar("T", bound=Aggregate)


class SyncRepository(Generic[T]):
    def __init__(self):
        self.seen: Set[T] = set()

    def get(self, id: str) -> T:
        agg: T = self._get(id=id)
        if agg:
            self.seen.add(agg)
        return agg

    def add(self, agg: T):
        self._add(agg)
        self.seen.add(agg)

    @abstractmethod
    def _get(self, id: str) -> T:
        raise NotImplementedError

    @abstractmethod
    def _add(self, aggregate: T):
        raise NotImplementedError


class AsyncRepository(Generic[T]):
    def __init__(self):
        self.seen: Set[T] = set()

    async def get(self, id: str) -> T:
        agg = await self._get(id=id)
        if agg:
            self.seen.add(agg)
        return agg

    async def get_list(self, **kwargs) -> List[T]:
        agg_list = await self._get_list(**kwargs)
        if agg_list:
            self.seen.update(agg_list)
        return agg_list

    async def add(self, agg: T):
        await self._add(agg)
        self.seen.add(agg)

    async def update(self, agg: T):
        await self._update(agg)
        self.seen.add(agg)

    @abstractmethod
    async def _get(self, id: str) -> T:
        raise NotImplementedError

    @abstractmethod
    async def _get_list(self, **kwargs) -> List[T]:
        raise NotImplementedError

    @abstractmethod
    async def _add(self, aggregate: T):
        raise NotImplementedError

    @abstractmethod
    async def _update(self, aggregate: T):
        raise NotImplementedError


Repository = Union[SyncRepository[T], AsyncRepository[T]]
