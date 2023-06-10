from __future__ import annotations
from abc import ABC

from typing import (
    Any,
    Generic,
    Iterable,
    Dict,
    Protocol,
    Type,
    TypeVar,
)

from cosmos.domain import AggregateRoot, Event
from cosmos.repository import AsyncRepository

T = TypeVar("T", bound=AggregateRoot)


class Collect(Protocol):
    """Callback protocol to provide an abstracted collect method."""

    def __call__(self, repository: AsyncRepository) -> Iterable[Event]:
        """An interface to collect events from a repository"""

        ...


class AsyncUnitOfWork(ABC):
    """A class dedicated to defining what one 'unit' of work is.

    This is an implementation of the unit of work pattern. Its core
    responsibility is providing a single context for a repository to
    share. If any repository action fails, all previous changes up to
    that point shall be reverted.
    """

    repository: AsyncRepository

    def __init__(self, *args, **kwargs) -> None:
        ...

    def collect_events(self) -> Iterable[Event]:
        for aggregate in self.repository.seen:
            while aggregate.has_events:
                yield aggregate.get_events().pop(0)

    async def __aenter__(self) -> AsyncUnitOfWork:
        raise NotImplementedError

    async def __aexit__(self, *args):
        raise NotImplementedError


class AsyncUnitOfWorkFactory(Generic[T]):
    """Factory implementation to create an async uow, from a uow cls and repo cls."""

    def __init__(
        self,
        uow_cls: Type[AsyncUnitOfWork],
        repository_cls: Type[AsyncRepository],
        uow_kwargs: Dict[Any, Any] | None = None,
        repository_kwargs: Dict[Any, Any] | None = None,
    ):
        """Takes a uow class and repo class, and saves for use in uow creation."""

        self._uow_cls = uow_cls
        self._uow_kwargs = uow_kwargs if uow_kwargs else {}
        self._repository_kwargs = repository_kwargs if repository_kwargs else {}
        self._repo_cls = repository_cls

    def get_uow(self) -> AsyncUnitOfWork:
        """Create and return a new uow instance."""

        uow = self._uow_cls(
            repository=self._repo_cls(**self._repository_kwargs),
            **self._uow_kwargs,
        ) 

        return uow
