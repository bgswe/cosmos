import pickle
from typing import List

import asyncpg
from cosmos.domain import Message


class PostgresOutbox:
    def __init__(self):
        self.connection: asyncpg.Connection = None

    async def send(self, messages: List[Message]):
        if self.connection is None:
            raise "This outbox requires setting the connection object before sending"

        for message in messages:
            message_id = message.message_id
            message = pickle.dumps(message)

            await self.connection.execute(
                f"""
                INSERT INTO
                    message_outbox (id, message) 
                VALUES
                    ($1, $2, $3);
                """,
                message_id,
                message,
            )
