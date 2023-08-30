from uuid import UUID

import asyncpg
from cosmos.unit_of_work import UnitOfWork
from cosmos.repository import AggregateRepository
from cosmos.unit_of_work import TransactionalOutbox


class PostgresUnitOfWork(UnitOfWork):
    def __init__(
        self,
        repository: AggregateRepository,
        outbox: TransactionalOutbox,
        pool: asyncpg.Pool,
    ):
        self.repository = repository
        self.outbox = outbox
        self.pool = pool

    async def __aenter__(self) -> UnitOfWork:
        self._pool_acquire_context = self.pool.acquire()

        self.connection = await self._pool_acquire_context.__aenter__()
        self._transaction = self.connection.transaction()

        await self._transaction.__aenter__()

        return self

    async def __aexit__(self, *args, **kwargs):
        """..."""

        await self.send_events_to_outbox()

        await self._transaction.__aexit__(*args, **kwargs)
        await self._pool_acquire_context.__aexit__(*args, **kwargs)

    async def send_events_to_outbox(self):
        """..."""

        events = [event for agg in self.repository.seen for event in agg.events]

        await self.outbox.send(connection=self.connection, messages=events)
