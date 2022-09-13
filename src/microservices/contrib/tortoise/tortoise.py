import logging
from functools import reduce
from typing import Any, Dict, Generic, List, Tuple, TypeVar

from asyncpg import connect
from tortoise import Tortoise
from tortoise.backends.base.client import TransactionContext
from tortoise.transactions import in_transaction

from microservices.contrib.pg.async_uow import AsyncUnitOfWorkPostgres
from microservices.domain import Aggregate
from microservices.repository import AsyncRepository
from microservices.unit_of_work import Collect


def simple_collect(repository: AsyncRepository):
    for aggregate in repository.seen:
        while aggregate.has_events:
            yield aggregate.get_events().pop(0)


async def tortoise_connect(
    generate: bool = False,
    db_url: str = None,
    models: List[str] = None,
):

    await Tortoise.init(
        db_url=db_url,
        modules={"models": models},
    )

    if generate:
        await Tortoise.generate_schemas()


def aggregate_related_object_fields(input: Dict[str, Dict]) -> Dict[str, Any]:
    output = {}
    split_keys = []

    for key in input.keys():
        if "__" in key:
            first, *rest = key.split("__")
            split_keys.append((first, "__".join(rest)))
        else:
            output[key] = input[key]

    def reducer(
        dictionary: Dict[str, Any], split_key: Tuple[str, str]
    ) -> Dict[str, Any]:
        """Groups remaining portions of split key, by first portion of split key."""

        first, rest = split_key

        # Check to see if list for first split key has been created
        rest_list = dictionary.get(first, None)
        if rest_list is not None:
            rest_list.append(rest)
        else:
            dictionary[first] = [rest]

        return dictionary

    split_keys_organized: Dict[str, Any] = reduce(reducer, split_keys, {})

    for first, rest in split_keys_organized.items():
        output[first] = {}

        # Map split key value back to input value
        for field in rest:
            output[first][field] = input[f"{first}__{field}"]

        # Run all remaining key portions through this function
        output[first] = aggregate_related_object_fields(output[first])

    return output


T = TypeVar("T", bound=Aggregate)


class TortoiseUOW(AsyncUnitOfWorkPostgres, Generic[T]):
    def __init__(
        self,
        # TODO: Evaluate this being unused??
        transaction_context: TransactionContext = None,
        repository: AsyncRepository[T] = None,
        collect: Collect = None,
    ):
        if collect is not None:
            self._collect = collect
        else:
            self._collect = simple_collect

        self._connection = connect()
        self._repo = repository

    async def __aenter__(self):
        self._tc = in_transaction()
        await self._tc.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        return await self._tc.__aexit__(*args, **kwargs)

    @property
    def repository(self) -> AsyncRepository:
        # TODO: Create a default repository to return
        # ignore added until above is implemented.

        return self._repo  # type: ignore

    @repository.setter
    def repository(self, value: AsyncRepository[T]):
        self._repo = value

    async def insert(self, sql: str, params: List[Any]) -> str | None:
        try:
            res = await self._tc.connection.execute_insert(sql, params)
        except Exception as e:
            logging.error("error running TortoiseUOW.insert")
            logging.error("Exception:", e)

        return res

    def collect_events(self):
        """..."""

        return self._collect(repository=self._repo)
