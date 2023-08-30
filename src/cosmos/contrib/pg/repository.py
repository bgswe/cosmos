from uuid import UUID

import asyncpg
from cosmos.domain import AggregateRoot
from cosmos.repository import AggregateEventStoreRepository
from cosmos.utils import json_encode


class PostgresEventStore(AggregateEventStoreRepository):
    """
    This is a general-use repository used to get or save event streams
    from/to a postgresql server.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.pool = pool

    async def _save(self, aggregate_root: AggregateRoot):
        current_version = getattr(aggregate_root, "_version", -1)

        # TODO: if -1, possible issue in AggRoot __init__ def

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                for event in aggregate_root.events:
                    d = event.model_dump()
                    message_id = d.pop("message_id")

                    current_version += 1

                    await connection.execute(
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
        async with self.pool.acquire() as connection:
            query = await connection.fetch(
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

            records = [record for record in query]

            # replay hydrated events to reconstruct current aggregate root state
            aggregate_root = self._replay_handler.replay(event_stream=records)

            # TODO: could add timing logs to gauge how long the hydration/replay lasts

            return aggregate_root
