from __future__ import annotations

from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Protocol,
    Type,
    TypeVar,
    runtime_checkable,
)

from cosmos.domain import AggregateRoot, Event
from cosmos.repository import AsyncRepository

T = TypeVar("T", bound=AggregateRoot)


class Collect(Protocol):
    """Callback protocol to provide an abstracted collect method."""

    def __call__(self, repository: AsyncRepository) -> Iterable[Event]:
        """An interface to collect events from a repository"""

        ...


@runtime_checkable
class AsyncUnitOfWork(Protocol[T]):
    """A class dedicated to defining what one 'unit' of work is.

    This is an implementation of the unit of work pattern. Its core
    responsibility is providing a single context for a repository to
    share. If any repository action fails, all previous changes up to
    that point shall be reverted.
    """

    def __init__(self, repository: AsyncRepository, collect: Collect):
        ...

    @property
    def repository(self) -> AsyncRepository:
        ...

    def collect_events(self) -> Iterator[Event]:
        ...

    async def __aenter__(self) -> AsyncUnitOfWork[T]:
        ...

    async def __aexit__(self, *args):
        ...

    async def transaction(self) -> Any:
        ...


class AsyncUnitOfWorkFactory(Generic[T]):
    """Factory implementation to create an async uow, from a uow cls and repo cls."""

    def __init__(
        self,
        uow_cls: Type[AsyncUnitOfWork[T]],
        repository_cls: Type[AsyncRepository[T]],
        collect: Collect|None = None,
    ):
        """Takes a uow class and repo class, and saves for use in uow creation."""

        self._uow_cls = uow_cls
        self._repo_cls = repository_cls
        self._collect = collect

    async def get_uow(self) -> AsyncUnitOfWork:
        """Create and return a new uow instance."""

        uow = self._uow_cls(
            repository=self._repo_cls(),
            collect=self._collect,  # type: ignore
        )

        connect = getattr(uow, "connect", None)

        if callable(connect):
            await uow.connect()  # type: ignore

        return uow
