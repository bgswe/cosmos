from datetime import datetime as dt, timezone
import json
from typing import Type
from uuid import UUID

import structlog

from cosmos.domain import AggregateRoot, DomainEvent
from cosmos.factory import Factory
from cosmos.repository import AggregateRepository


logger = structlog.get_logger()


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
            event_data = event.serialize()

            await self.connection.execute(
                f"""
                INSERT INTO
                    events(created, id, stream_id, type, data)
                VALUES
                    ($1, $2, $3, $4, $5);
                """,
                event.created,
                str(event.message_id),
                str(event.stream_id),
                event.type_name,
                event_data,
            )

    async def _get(
        self, id: UUID, aggregate_root_class: Type[AggregateRoot]
    ) -> Type[AggregateRoot] | None:
        if self.connection is None:
            # TODO: custom exception
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

        events = []
        for record in query:
            event_json = json.loads(record["data"])
            event_cls = DomainEvent.get_event_class(type=event_json["meta"]["type"])
            events.append(event_cls.model_validate(event_json["data"]))

        if not events:
            return

        return aggregate_root_class.replay(events=events)


class PostgresEventStoreFactory(Factory):
    def __init__(self, event_store_kwargs: dict, *args, **kwargs):
        self._event_store_kwargs = event_store_kwargs

    def get(self) -> PostgresEventStore:
        return PostgresEventStore(**self._event_store_kwargs)
