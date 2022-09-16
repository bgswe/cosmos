from typing import Generic, List, TypeVar

from asyncpg import connect
from tortoise import Tortoise  # type: ignore
from tortoise.transactions import in_transaction  # type: ignore

from microservices.contrib.pg.async_uow import AsyncUnitOfWorkPostgres
from microservices.domain import Aggregate
from microservices.repository import AsyncRepository
from microservices.unit_of_work import Collect

T = TypeVar("T", bound=Aggregate)


# EVAL: Do we need to expose collection here? I think this might be
# a 'Zero Value-Added' complexitiy.
def simple_collect(repository: AsyncRepository):
    """Default collection implementation."""

    for aggregate in repository.seen:
        while aggregate.has_events:
            yield aggregate.get_events().pop(0)


async def tortoise_connect(
    db_url: str,
    models: List[str],
    generate: bool = False,
):
    """Small connection utilitity for the tortoise-orm library."""

    await Tortoise.init(
        db_url=db_url,
        modules={"models": models},  # EVAL: is this config sufficient?
    )

    if generate:
        await Tortoise.generate_schemas()


class TortoiseUOW(AsyncUnitOfWorkPostgres, Generic[T]):
    """UnitofWork implmentation utilizing the tortoise-orm library."""

    def __init__(
        self,
        repository: AsyncRepository[T],
        collect: Collect = None,
    ):
        if collect is not None:
            self._collect = collect
        else:
            self._collect = simple_collect

        self._repository = repository

    async def connect(self):
        """Allows connection to be set to async return value.

        This initialization cannot be done in __init__ as connect is cannot be async.
        This method must be called after creating a new instance of TortoiseUOW.
        """

        self._connection = await connect()

    async def __aenter__(self):
        """Enter method for use as Async Context Manager.

        Utilizes tortoise-orm's 'in_transaction' function in a 'wrapper' manner.
        """

        self._tc = in_transaction()
        await self._tc.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        """Exit method for use as Async Context Manager, delegating to tortoise-orm."""

        return await self._tc.__aexit__(*args, **kwargs)

    @property
    def repository(self) -> AsyncRepository:
        """Simple getter for UnitOfWork's repository."""

        return self._repository

    def collect_events(self):
        """Wrapper method around the provided collect function."""

        return self._collect(repository=self._repo)
