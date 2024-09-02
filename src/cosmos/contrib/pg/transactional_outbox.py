import pickle

import asyncpg
from cosmos.domain import Message


class PostgresOutbox:
    def __init__(self):
        self.connection: asyncpg.Connection | None = None

    async def send(self, messages: list[Message]):
        if self.connection is None:
            # TODO: Raise custom exception
            raise Exception("This outbox requires a connection object before delivery")

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
