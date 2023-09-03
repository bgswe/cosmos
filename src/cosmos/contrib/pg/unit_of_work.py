from contextlib import AsyncExitStack
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

        self._stack = None

        super().__init__(**kwargs)

    async def __aenter__(self) -> UnitOfWork:
        """Entry into the async ctx manager for Postgres transaction"""

        async with AsyncExitStack() as stack:
            # acquire a new connection from pool, and begin a transaction
            connection = await stack.enter_async_context(self.pool.acquire())
            await stack.enter_async_context(connection.transaction())

            # provide connection to outbox, and repository so that
            # they are ran under a single transaction
            self.outbox.connection = connection
            self.repository.connection = connection

            # transfer __aexit__ callback stack so it may called in this obj's __aexit__
            self._stack = stack.pop_all()

        return self

    async def __aexit__(self, exc_type, exc, traceback):
        """Exit method of the async ctx manager for Postgres Transaction"""

        # save all new domain events to the transactional outbox
        events = [event for agg in self.repository.seen for event in agg.events]
        await self.outbox.send(messages=events)

        # reset outbox connection, and reset seen aggregates in repository
        self.outbox.connection = None
        self.repository.connection = None
        self.repository.reset()

        await self._stack.__aexit__(exc_type, exc, traceback)
