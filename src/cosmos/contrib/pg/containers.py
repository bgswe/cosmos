from typing import Dict
from uuid import UUID
from cosmos.domain import AggregateRoot
from cosmos.repository import AggregateReplay, AggregateRepository, EventHydrator
from cosmos.contrib.pg import (
    PostgresOutbox,
    PostgresEventStore,
    PostgresUnitOfWork,
    PostgresProcessedMessageRepository,
)
from dependency_injector import containers, providers
import asyncpg
from cosmos.unit_of_work import UnitOfWork


async def generate_postgres_pool(
    database,
    user,
):
    pool = await asyncpg.create_pool(
        database=database,
        user=user,
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
    )

    event_hydrator = providers.Factory(
        EventHydrator,
        aggregate_root_mapping=config.aggregate_root_mapping,
        event_hydration_mapping=config.event_hydration_mapping,
    )

    replay_handler = providers.Factory(
        AggregateReplay,
        event_hydrator=event_hydrator,
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
