from typing import Dict
from uuid import UUID

import asyncpg
from cosmos.domain import AggregateRoot
from cosmos.repository import AggregateRepository
from cosmos.contrib.pg import (
    PostgresOutbox,
    PostgresEventStore,
    PostgresUnitOfWork,
    PostgresProcessedMessageRepository,
)
from cosmos.unit_of_work import UnitOfWork
from dependency_injector import containers, providers


async def generate_postgres_pool(
    database,
    user,
    host,
    password,
    port,
):
    pool = await asyncpg.create_pool(
        database=database,
        user=user,
        host=host,
        password=password,
        port=port,
    )

    yield pool

    pool.close()


class MockAggregateStore:
    def __init__(self):
        self._store = {}

    def get(self, id: UUID) -> AggregateRoot | None:
        return self._store.get(id)

    def save(self, agg: AggregateRoot):
        self._store[agg.id] = agg


class MockRepository(AggregateRepository):
    def __init__(self, aggregate_store: Dict):
        super().__init__()

        self._store = aggregate_store

    async def _get(self, id: UUID) -> AggregateRoot | None:
        return self._store.get(id)

    async def _save(self, agg: AggregateRoot):
        self._store[agg.id] = agg


class MockUnitOfWork(UnitOfWork):
    async def __aenter__(self) -> UnitOfWork:
        print("__aenter__ from MockUnitOfWork")
        return self

    async def __aexit__(self, *args):
        print("__aexit__ from MockUnitOfWork")


class MockDomainContainer(containers.DeclarativeContainer):
    aggregate_store = providers.Singleton(MockAggregateStore)

    repository = providers.Factory(
        MockRepository,
        aggregate_store=aggregate_store,
    )

    unit_of_work = providers.Factory(
        MockUnitOfWork,
        repository=repository,
    )


class PostgresDomainContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    connection_pool = providers.Resource(
        generate_postgres_pool,
        database=config.database_name,
        user=config.database_user,
        host=config.database_host,
        password=config.database_password,
        port=config.database_port,
    )

    repository = providers.Factory(
        PostgresEventStore,
        replay_handler=replay_handler,
    )

    outbox = providers.Factory(
        PostgresOutbox,
    )

    processed_messages = providers.Factory(
        PostgresProcessedMessageRepository,
    )

    unit_of_work = providers.Factory(
        PostgresUnitOfWork,
        processed_message_repository=processed_messages,
        pool=connection_pool,
        repository=repository,
        outbox=outbox,
    )
