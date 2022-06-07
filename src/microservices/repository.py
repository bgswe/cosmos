from abc import ABC, abstractmethod
from typing import Set, TypeVar, Union

from .domain import Aggregate

T = TypeVar("T", bound=Aggregate)


class SyncRepository(ABC):
    def __init__(self):
        self.seen: Set[T] = set()

    def get(self, id: str) -> T:
        agg = self._get(id=id)
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


class AsyncRepository(ABC):
    def __init__(self):
        self.seen: Set[T] = set()

    async def get(self, id: str) -> T:
        agg = await self._get(id=id)
        if agg:
            self.seen.add(agg)
        return agg

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
    async def _add(self, aggregate: T):
        raise NotImplementedError

    @abstractmethod
    async def _add(self, aggregate: T):
        raise NotImplementedError

Repository = Union[SyncRepository, AsyncRepository]
