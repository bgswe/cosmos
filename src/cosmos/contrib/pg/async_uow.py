from abc import ABC

from asyncpg import Connection

from cosmos.repository import AsyncRepository
from cosmos.unit_of_work import AsyncUnitOfWork


class AsyncPGRepository(AsyncRepository, ABC):
    def __init__(self, connection: Connection):
        self.connection = connection

        super().__init__()


class AsyncUnitOfWorkPostgres(AsyncUnitOfWork):
    def __init__(self, connection: Connection):
        self.connection = connection

    async def __aenter__(self, *args, **kwargs) -> AsyncUnitOfWork:
        self.transaction = self.connection.transaction()
        await self.transaction.__aenter__(*args, **kwargs)
        return self

    async def __aexit__(self, *args, **kwargs):
        return await self.transaction.__aexit__(*args, **kwargs)
