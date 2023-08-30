from typing import List

import asyncpg
from cosmos.domain import Message
from cosmos.utils import json_encode


class PostgresOutbox:
    async def send(self, messages: List[Message], connection: asyncpg.Connection):
        for message in messages:
            d = message.model_dump()
            message_id = d.pop("message_id")

            await connection.execute(
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
