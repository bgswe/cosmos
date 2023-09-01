from uuid import UUID

import asyncpg
from cosmos.unit_of_work import UnitOfWork


class PostgresUnitOfWork(UnitOfWork):
    def __init__(
        self,
        pool: asyncpg.Pool,
        **kwargs,
    ):
        self.pool = pool

        super().__init__(**kwargs)

    async def __aenter__(self) -> UnitOfWork:
        """Entry into the async ctx manager for Postgres transaction"""

        self._pool_acquire_context = self.pool.acquire()

        # leverage async ctx manager from asyncpg to get connection,
        # and initiate a new transaction
        self.connection = await self._pool_acquire_context.__aenter__()
        self._transaction = self.connection.transaction()
        await self._transaction.__aenter__()

        # provide outbox with the DB connection, under the same transaction
        self.outbox.connection = self.connection

        return self

    async def __aexit__(self, *args, **kwargs):
        """Exit method of the async ctx manager for Postgres Transaction"""

        # before commiting transaction, save all found domain events to the outbox
        events = [event for agg in self.repository.seen for event in agg.events]
        await self.outbox.send(messages=events)

        # cleanup duties
        self.outbox.connection = None
        self.repository.reset()

        await self._transaction.__aexit__(*args, **kwargs)
        await self._pool_acquire_context.__aexit__(*args, **kwargs)
