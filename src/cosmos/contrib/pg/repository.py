import pickle
from typing import Type
from uuid import UUID

from cosmos.domain import AggregateRoot
from cosmos.factory import Factory
from cosmos.repository import AggregateRepository


class PostgresEventStore(AggregateRepository):
    """Create/Read aggregates from an event store implmentation in PostgreSQL"""

    async def _save(self, aggregate_root: AggregateRoot):
        current_version = getattr(aggregate_root, "_version", -1)
        # TODO: if -1, possible issue in AggRoot __init__ def

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
                str(aggregate_root.id),
                pickled_event,
            )

    async def _get(self, id: UUID, aggregate_root_class: Type[AggregateRoot]):
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
    def get(self) -> PostgresEventStore:
        return PostgresEventStore()
