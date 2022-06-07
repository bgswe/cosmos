from abc import abstractmethod
from typing import Any, Dict, List, Protocol, Union

from microservices.domain import Event
from microservices.repository import AsyncRepository, Repository


class EventCollector(Protocol):
    @abstractmethod
    def collect(repository: Repository) -> List[Event]:
        raise NotImplementedError


class BaseUnitOfWork(Protocol):
    repository: AsyncRepository
    _event_collector: EventCollector

    def collect_events(self) -> List[Event]:
        self._event_collector.collect()


class SyncUnitOfWork(BaseUnitOfWork, Protocol):
    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *args):
        raise NotImplementedError


class AsyncUnitOfWork(BaseUnitOfWork, Protocol):
    @abstractmethod
    def __aenter__(self):
        raise NotImplementedError

    @abstractmethod
    def __aexit__(self, *args):
        raise NotImplementedError

    @abstractmethod
    def query(self, query: str, params: List[Any]) -> List[Dict[str, Any]]:
        raise NotImplementedError


UnitOfWork = Union[SyncUnitOfWork, AsyncUnitOfWork]
