from abc import ABC

from asyncpg import connect
from asyncpg.transaction import Transaction


class AsyncUnitOfWorkPostgres(ABC):
    async def transaction(self) -> Transaction:
        """ABC with transaction implementation for postgres.

        The connection uses default environment variables to
        configure the connection string.
        They include (default):
            PGHOST     (localhost)
            PGPORT     (5432)
            PGUSER     (OS SYSTEM USER)
            PGDATABASE (PGUSER)
            PGPASSWORD
        """

        if not self._connection:
            self._connection = await connect()

        return self._connection.transaction()
