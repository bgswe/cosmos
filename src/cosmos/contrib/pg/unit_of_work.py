from contextlib import AsyncExitStack
from typing import Dict
from uuid import UUID

import asyncpg
from cosmos.unit_of_work import UnitOfWork


class PostgresProcessedMessageRepository:
    """
    This is a general-use repository used to get or save event streams
    from/to a postgresql server.
    """

    def __init__(self):
        self.connection = None

    async def is_processed(self, message_id: UUID):
        query = await self.connection.fetchrow(
            f"""
            SELECT EXISTS (
                SELECT
                    1
                FROM
                    processed_messages
                where
                    id = $1
            )
            """,
            str(message_id),
        )

        return query["exists"]

    async def mark_processed(self, message_id: UUID):
        await self.connection.execute(
            f"""
            INSERT INTO
                processed_messages (id) 
            VALUES
                ($1);
            """,
            str(message_id),
        )


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
            self.processed_messages.connection = connection

            # transfer __aexit__ callback stack so it may called in this obj's __aexit__
            self._stack = stack.pop_all()

        return self

    async def __aexit__(self, exc_type, exc, traceback):
        """Exit method of the async ctx manager for Postgres Transaction"""

        # save all new domain events to the transactional outbox
        events = [event for agg in self.repository.seen for event in agg.events]
        await self.outbox.send(messages=events)

        # reset connection object within dependent objects
        self.outbox.connection = None
        self.repository.connection = None
        self.processed_messages.connection = None

        # reset state of repository after end of transaction
        self.repository.reset()

        await self._stack.__aexit__(exc_type, exc, traceback)
