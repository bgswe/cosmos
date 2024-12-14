import pickle
from typing import Type
from uuid import UUID

from cosmos.domain import AggregateRoot
from cosmos.factory import Factory
from cosmos.repository import AggregateRepository


class PostgresEventStore(AggregateRepository):
    """Create/Read aggregates from an event store implmentation in PostgreSQL"""

    def __init__(self, *args, **kwargs):
        # TODO: mark asyncpg as peerDep? Is that the correct term?
        self.connection: asyncpg.Connection | None = None
        super().__init__(*args, **kwargs)

    async def _save(self, aggregate_root: AggregateRoot):
        if self.connection is None:
            raise Exception("This repository requires a connection object before save")

        current_version = getattr(aggregate_root, "_version", -1)
        # TODO: if -1, possible issue in AggRoot __init__ def
        # TODO: Do we have version checks? Needed for preventing event race-conditions
        for event in aggregate_root.events:
            current_version += 1
            pickled_event = pickle.dumps(event)

            await self.connection.execute(
                f"""
                INSERT INTO
                    events(id, stream_id, data) 
                VALUES
                    ($1, $2, $3);
                """,
                str(event.message_id),
                str(event.stream_id),
                pickled_event,
            )

    async def _get(self, id: UUID, aggregate_root_class: Type[AggregateRoot]):
        if self.connection is None:
            raise Exception("This repository requires a connection object before save")

        query = await self.connection.fetch(
            f"""
            SELECT
                data
            FROM
                events
            WHERE
                stream_id = $1
            """,
            str(id),
        )

        events = [pickle.loads(record["data"]) for record in query]
        return aggregate_root_class.replay(events=events)


class PostgresEventStoreFactory(Factory):
    def __init__(self, event_store_kwargs: dict, *args, **kwargs):
        self._event_store_kwargs = event_store_kwargs

    def get(self) -> PostgresEventStore:
        return PostgresEventStore(**self._event_store_kwargs)
