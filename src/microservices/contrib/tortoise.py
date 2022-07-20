import logging
from abc import abstractmethod
from functools import reduce
from typing import Any, Dict, List, Protocol, Tuple

from tortoise import Model, fields
from tortoise.backends.base.client import TransactionContext
from tortoise.transactions import in_transaction

from microservices.repository import AsyncRepository, Repository


class TimestampMixin:
    created_at = fields.DatetimeField(null=True, auto_now_add=True)
    modified_at = fields.DatetimeField(null=True, auto_now=True)


class BaseMixin:
    # TODO: Validate this to be UUIDv4
    id = fields.CharField(max_length=36, pk=True, generated=False)  # UUIDv4


class AbstractBaseModel(Model, BaseMixin, TimestampMixin):
    class Meta:
        abstract = True


class SimpleEventCollector:
    def collect(self, repository: Repository):
        for aggregate in repository.seen:
            while aggregate.has_events:
                yield aggregate.get_events().pop(0)


def aggregate_related_object_fields(input: Dict[str, Any]) -> Dict[str, Any]:
    output = {}
    split_keys = []

    for key in input.keys():
        if "__" in key:
            first, *rest = key.split("__")
            split_keys.append((first, "__".join(rest)))
        else:
            output[key] = input[key]

    def reducer(dictionary: Dict[str, Any], split_key: Tuple[str]):
        """Groups remaining portions of split key, by first portion of split key."""
        first, rest = split_key

        # Check to see if list for first split key has been created
        rest_list = dictionary.get(first, None)
        if rest_list is not None:
            rest_list.append(rest)
        else:
            dictionary[first] = [rest]

        return dictionary

    split_keys_organized = reduce(reducer, split_keys, {})

    for first, rest in split_keys_organized.items():
        output[first] = {}

        # Map split key value back to input value
        for field in rest:
            output[first][field] = input[f"{first}__{field}"]

        # Run all remaining key portions through this function
        output[first] = aggregate_related_object_fields(output[first])

    return output


class Collector(Protocol):
    @abstractmethod
    def collect(self, repository: Repository):
        raise NotImplementedError


class SimpleCollector:
    def collect(self, repository: Repository):
        for aggregate in repository.seen:
            while aggregate.has_events:
                yield aggregate.get_events().pop(0)


class TortoiseUOW:
    def __init__(
        self,
        transaction_context: TransactionContext = None,
        repository: Repository = None,
        collector: Collector = None,
    ):
        if collector is None:
            self._collector = SimpleCollector()
        else:
            self._collector = collector

        self._repo = repository

    async def __aenter__(self):
        self._tc = in_transaction()
        await self._tc.__aenter__()
        return self

    async def __aexit__(self, *args, **kwargs):
        return await self._tc.__aexit__(*args, **kwargs)

    @property
    def repository(self) -> AsyncRepository:
        return self._repo

    async def query(self, query: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        try:
            row_count, results = await self._tc.connection.execute_query(query, params)
        except Exception as e:
            logging.error("error running TortoiseUOW.query")
            logging.error("Exception:", e)

        return (row_count, [aggregate_related_object_fields(r) for r in results])

    async def insert(self, sql: str, params: List[Any]) -> str | None:
        try:
            res = await self._tc.connection.execute_insert(sql, params)
        except Exception as e:
            logging.error("error running TortoiseUOW.insert")
            logging.error("Exception:", e)

        return res

    def collect_events(self):
        if self._collector is None:
            # TODO: Customer Exception
            raise Exception

        return self._collector.collect(repository=self._repo)
