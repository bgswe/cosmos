from __future__ import annotations

from typing import Iterator, Protocol, Type, TypeVar, runtime_checkable

from microservices.domain import Aggregate
from microservices.events import Event
from microservices.repository import AsyncRepository

T = TypeVar("T", bound=Aggregate)


@runtime_checkable
class AsyncUnitOfWork(Protocol[T]):
    """A class dedicated to defining what one 'unit' of work is.

    This is an implementation of the unit of work pattern. Its core
    responsibility is providing a single context for a repository to
    share. If any repository action fails, all previous changes up to
    that point shall be reverted.

    TODO: Design way to handle multiple repositories. This will support
    each entity having a repository if necessary.
    """

    def __init__(self, repository: AsyncRepository[T]):
        """Implementation must include repository in its init."""

        ...

    def repository(self, repository) -> AsyncRepository:

        ...

    def collect_events(self) -> Iterator[Event]:
        """Implementation must include a method to gather events from repository."""

        ...

    async def __aenter__(self) -> AsyncUnitOfWork:
        """Implementation must implement async context manager enter."""

        ...

    async def __aexit__(self, *args):
        """Implementation must implement async context manager exit."""

        ...

    # TODO: REMOVE THIS COMMENT. Leaving temporarily while
    # redesigning how to use sql statements.
    # @abstractmethod
    # async def query(
    #     self, query: str, params: List[Any] = None
    # ) -> Tuple[int, List[Dict[str, Any]]]:
    #     raise NotImplementedError


class AsyncUnitOfWorkFactory:
    """Factory implementation to create an async uow, from a uow cls and repo cls."""

    def __init__(
        self,
        uow_cls: Type[AsyncUnitOfWork],
        repository_cls: Type[AsyncRepository],
    ):
        """Takes a uow class and repo class, and saves for use in uow creation."""

        self._uow_cls = uow_cls
        self._repo_cls = repository_cls

    def get_uow(self) -> AsyncUnitOfWork:
        """Create and return a new uow instance."""

        return self._uow_cls(repository=self._repo_cls())
