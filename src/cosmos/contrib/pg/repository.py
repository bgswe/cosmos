from typing import Type
from uuid import UUID

from cosmos.domain import AggregateRoot
from cosmos.repository import AggregateEventStoreRepository
from cosmos.utils import json_encode


class PostgresEventStore(AggregateEventStoreRepository):
    """
    This is a general-use repository used to get or save event streams
    from/to a postgresql server.
    """

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
                    events(id, stream_id, stream_type, event_type, version, data) 
                VALUES
                    ($1, $2, $3, $4, $5, $6);
                """,
                str(message_id),
                str(aggregate_root.id),
                aggregate_root.name,
                event.name,
                current_version,
                json_encode(data=d),
            )

    async def _get(self, id: UUID, aggregate_root_class: Type[AggregateRoot]):
        query = await self.connection.fetch(
            f"""
            SELECT
                id, stream_id, stream_type, event_type, version, data
            FROM
                events
            WHERE
                stream_id = $1
            """,
            id,
        )

        events = [record for record in query]
        return aggregate_root_class.replay(events=events)
