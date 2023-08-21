from abc import ABC
import json
from typing import Type
from uuid import UUID

from asyncpg import Connection
from cosmos.domain import AggregateRoot

from cosmos.repository import AsyncRepository
from cosmos.unit_of_work import AsyncUnitOfWork
from cosmos.utils import json_encode


class AsyncPGRepository(AsyncRepository, ABC):
    def __init__(self, connection: Connection):
        self.connection = connection

        super().__init__()


class AsyncPGEventStoreRepository(AsyncPGRepository):
    """
    This repository is shaping up to be usable for any aggregate in the system,
    if given a proper replay function.
    """

    def __init__(self, AggregateRootClass: AggregateRoot, **kwargs):
        super().__init__(**kwargs)

        self._AggregateRootClass = AggregateRootClass

    async def _save(self, aggregate_root: AggregateRoot):
        current_version = getattr(aggregate_root, "_version", -1)

        # TODO: if -1, possible issue in AggRoot __init__ def

        for event in aggregate_root.events:
            d = event.model_dump()
            message_id = d.pop("message_id")

            current_version += 1

            # TODO: unwanted model_config attr coming thru dict,
            # TODO: see if we can remove the need for this .pop()
            d.pop("model_config")

            await self.connection.execute(
                f"""
                INSERT INTO
                    events(id, stream_id, type, version, data) 
                VALUES
                    ($1, $2, $3, $4, $5);
                """,
                message_id,
                aggregate_root.id,
                event.name,
                current_version,
                json_encode(data=d),
            )

    async def _get(self, id: UUID):
        query = await self.connection.fetch(
            f"""
            SELECT
                id, stream_id, type, version, data
            FROM
                events
            WHERE
                stream_id = $1
            """,
            id,
        )

        # hydrate event objects based on str:EventClass pairs provided by the AggregateRootClass
        events = [
            self._AggregateRootClass.EVENT_TYPES[record["type"]](
                message_id=record["id"], **(json.loads(record["data"]))
            )
            for record in query
        ]

        # replay hydrated events to reconstruct current aggregate root state
        aggregate_root = self._AggregateRootClass.replay(events=events)

        # TODO: could add timing logs to gauge how long the hydration/replay lasts

        return aggregate_root


class AsyncUnitOfWorkPostgres(AsyncUnitOfWork):
    def __init__(self, connection: Connection):
        self.connection = connection

    def context(self, AggregateRootClass: Type):
        self.repository = AsyncPGEventStoreRepository(
            connection=self.connection,
            AggregateRootClass=AggregateRootClass,
        )

        return self

    async def __aenter__(self, *args, **kwargs) -> AsyncUnitOfWork:
        self.transaction = self.connection.transaction()
        await self.transaction.__aenter__(*args, **kwargs)
        return self

    async def __aexit__(self, *args, **kwargs):
        return await self.transaction.__aexit__(*args, **kwargs)
