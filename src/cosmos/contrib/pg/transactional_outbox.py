import pickle

import asyncpg
from cosmos.domain import Message


class PostgresOutbox:
    def __init__(self):
        self.connection: asyncpg.Connection = None

    async def send(self, messages: list[Message]):
        if self.connection is None:
            raise "This outbox requires setting the connection object before sending"

        # TODO: batch insert the list of messages
        for message in messages:
            message_id = message.message_id
            message = pickle.dumps(message)

            await self.connection.execute(
                f"""
                INSERT INTO
                    message_outbox (id, message) 
                VALUES
                    ($1, $2);
                """,
                str(message_id),
                message,
            )
