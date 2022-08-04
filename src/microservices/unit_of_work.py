from __future__ import annotations

from abc import abstractmethod
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from microservices.domain import Aggregate, Event
from microservices.repository import AsyncRepository, Repository

EventCollector = Callable[[Repository], Iterator[Event]]


def simple_collector(repository: Repository) -> Iterator[Event]:
    for aggregate in repository.seen:
        while aggregate.has_events:
            # I believe we yield to allow the handling of this event
            # to generate events itself
            yield aggregate.get_events().pop(0)


T = TypeVar("T", bound=Aggregate)


class BaseUnitOfWork(Protocol[T]):
    repository: AsyncRepository[T]

    @abstractmethod
    def __init__(self, repository: Repository):
        raise NotImplementedError

    @abstractmethod
    def collect_events(self) -> Iterator[Event]:
        raise NotImplementedError


class SyncUnitOfWork(BaseUnitOfWork, Protocol):
    @abstractmethod
    def __enter__(self):
        raise NotImplementedError

    @abstractmethod
    def __exit__(self, *args):
        raise NotImplementedError


# EVAL: Has error where covariant, and non-covariant Protocol throws err
class AsyncUnitOfWork(BaseUnitOfWork, Protocol[T]):  # type: ignore
    @abstractmethod
    async def __aenter__(self) -> AsyncUnitOfWork:
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, *args):
        raise NotImplementedError

    @abstractmethod
    async def query(
        self, query: str, params: List[Any] = None
    ) -> Tuple[int, List[Dict[str, Any]]]:
        raise NotImplementedError


UnitOfWork = Union[SyncUnitOfWork, AsyncUnitOfWork]


class UnitOfWorkFactory(Protocol):
    @abstractmethod
    def get_uow(self) -> UnitOfWork:
        raise NotImplementedError


class AsyncUOWFactory:
    def __init__(
        self, uow_cls: Type[AsyncUnitOfWork], repository_cls: Type[Repository]
    ):
        self._uow_cls = uow_cls
        self._repo_cls = repository_cls

    def get_uow(self) -> AsyncUnitOfWork:
        return self._uow_cls(repository=self._repo_cls())
