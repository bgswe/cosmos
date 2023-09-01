from typing import List

import asyncpg
from cosmos.domain import Message
from cosmos.utils import json_encode


class PostgresOutbox:
    def __init__(self):
        self.connection: asyncpg.Connection = None

    async def send(self, messages: List[Message]):
        if self.connection is None:
            raise "This outbox requires setting the connection object before sending"

        for message in messages:
            d = message.model_dump()
            message_id = d.pop("message_id")

            await self.connection.execute(
                f"""
                INSERT INTO
                    message_outbox (id, type, data) 
                VALUES
                    ($1, $2, $3, $4, $5);
                """,
                message_id,
                message.name,
                json_encode(data=d),
            )
