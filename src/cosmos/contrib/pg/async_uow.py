from uuid import UUID

from asyncpg import Connection
from cosmos.domain import AggregateRoot

from cosmos.repository import AggregateEventStoreRepository
from cosmos.unit_of_work import UnitOfWork
from cosmos.utils import json_encode


class PostgresEventStore(AggregateEventStoreRepository):
    """
    This is a general-use repository used to get or save event streams
    from/to a postgresql server.
    """

    def __init__(
        self,
        connection: Connection,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.connection = connection

    async def _save(self, aggregate_root: AggregateRoot):
        current_version = getattr(aggregate_root, "_version", -1)

        # TODO: if -1, possible issue in AggRoot __init__ def

        for event in aggregate_root.events:
            d = event.model_dump()
            message_id = d.pop("message_id")

            current_version += 1

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

        records = [record for record in query]

        # TODO:CRITICAL: what does the event_record look like coming back from here?
        print(records)

        # replay hydrated events to reconstruct current aggregate root state
        aggregate_root = self._replay_handler.replay(event_stream=records)

        # TODO: could add timing logs to gauge how long the hydration/replay lasts

        return aggregate_root


class PostgresUnitOfWork(UnitOfWork):
    def __init__(
        self,
        connection: Connection,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.connection = connection

    async def __aenter__(self, *args, **kwargs) -> UnitOfWork:
        self.transaction = self.connection.transaction()
        await self.transaction.__aenter__(*args, **kwargs)
        return self

    async def __aexit__(self, *args, **kwargs):
        """..."""

        await self.send_events_to_outbox()

        await self.transaction.__aexit__(*args, **kwargs)
        self.transaction = None
