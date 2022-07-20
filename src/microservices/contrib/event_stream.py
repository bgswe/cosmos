import logging
from dataclasses import asdict

import aiohttp

from microservices.domain import Event
from microservices.unit_of_work import AsyncUnitOfWork


class EventStreamPublisher:
    def __init__(self, uow: AsyncUnitOfWork, event_stream_url: str):
        self._uow = uow
        self._event_stream_url = event_stream_url

    async def publish(self, event: Event) -> None:
        async with self._uow:
            # TODO: Commit to DB for local backup
            # of events in case publish fails

            async with aiohttp.ClientSession() as session:
                event_dict = asdict(obj=event)
                event_id = event_dict.pop("id")

                if event_id is None:
                    logging.error(
                        f"'event_id' is None for publish of event {event.id}"
                        f"to {event.stream}"
                    )
                    return

                async with session.post(
                    f"{self._event_stream_url}/publish",
                    json={
                        "id": event_id,
                        "stream": event.stream.value,
                        "payload": event_dict,
                    },
                ) as resp:
                    if resp.status != 200:
                        logging.error("issue publishing to event stream")
