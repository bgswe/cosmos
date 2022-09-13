import asyncio
import json
import os
from typing import Any, Dict, Protocol, Tuple

from redis import Redis

from microservices.domain import Consumer
from microservices.events import STREAM_TO_EVENT_MAPPING, Event
from microservices.message_bus import MessageBus
from microservices.messages import (
    Domain,
    DomainConsumerConfig,
    EventConsume,
    EventPublish,
)
from microservices.unit_of_work import AsyncUnitOfWorkFactory
from microservices.utils import get_logger

logger = get_logger()


class RedisClient(Protocol):
    """Interface to abstract away dependency on current redis client."""

    def xadd(
        self,
        name: Any,
        fields: Any,
        id: str = "*",
        maxlen=None,
        approximate=True,
        nomkstream=False,
        minid=None,
        limit=None,
    ):
        ...

    def xread(self, stream: Dict[str, str], count: int):
        ...


def get_redis_client() -> Redis:
    """Encapsulated configuration management for redis-py client."""

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

    # Establish Redis client connection.
    return Redis(
        host=REDIS_HOST, port=REDIS_PORT, retry_on_timeout=True, decode_responses=True
    )


def redis_publisher(client: RedisClient) -> EventPublish:
    def publish(event: Event):
        """EventPublish implementation for the redis stream stack."""

        values = event.dict()

        log = logger.bind(event=values)

        try:
            response = client.xadd(
                name=event.stream.value,
                fields={"values": json.dumps({values})},
            )

            log = log.bind(publish_response=response)
            log.info("successfully published event to redis stream")

        except Exception as e:
            log = logger.bind(exception_type=type(e))
            log.error("error attempting to publish event")

            # TODO: What todo when message publish fails?

    return publish


async def consume(
    domain: Domain,
    config: DomainConsumerConfig,
    uow_factory: AsyncUnitOfWorkFactory,
    consumer_uow_factory: AsyncUnitOfWorkFactory[Consumer],
    event_consume: EventConsume,
    event_publish: EventPublish,
):
    """Consumes events defined in config from redis streams."""

    # Create an event handler dict from the DomainConsumerConfig
    event_handlers = {}
    for k, v in config.items():
        event_handlers[k] = [consumer_config.handler for consumer_config in v]

    bus = MessageBus(
        domain=domain,
        uow_factory=uow_factory,
        event_handlers=event_handlers,
        event_publish=EventPublish,
    )

    # Create consumer sets representing consumer config, and current consumer state
    async with uow_factory.get_uow() as uow:
        current_consumer_names = {c.name for c in await uow.repository.get_list()}

    # Represent the new consumers we must now create
    created_consumers = []
    for stream, configs in config.items():
        new_configs = filter(lambda c: c.name not in current_consumer_names, configs)
        created_consumers.extend(
            [
                Consumer.create(stream=stream, name=c.name, retroactive=c.retroactive)
                for c in new_configs
            ]
        )

    # Create the new consumers if necessary
    async with uow_factory.get_uow() as uow:
        for c in created_consumers:
            await uow.repository.add(c)

    # Get all consumers from repo
    async with uow_factory.get_uow() as uow:
        consumers = await uow.repository.get_list()

    # Create an endlessly-looped event consumer for each individual consumer
    loop = asyncio.new_event_loop()

    async_tasks = [
        loop.create_task(
            loop_event_consumer(
                bus=bus,
                uow_factory=consumer_uow_factory,
                event_consume=event_consume,
                consumer=consumer,
            )
        )
        for consumer in consumers
    ]

    await asyncio.wait(async_tasks)


async def loop_event_consumer(
    bus: MessageBus,
    uow_factory: AsyncUnitOfWorkFactory[Consumer],
    event_consume: EventConsume,
    consumer: Consumer,
):
    """Manages the inifinte looping of a given event consumer."""

    while True:
        try:
            async with uow_factory.get_uow() as uow:
                consumer = await uow.repository.get(id=consumer.id)

                consumer_response = await event_consume(consumer=consumer)

                if consumer_response:
                    event, message_id = consumer_response

                    await bus.handle(message=event)

                    consumer.acked_id = message_id
                    await uow.repository.update(consumer)

            await asyncio.sleep(3)

        except Exception as e:
            # TODO: Logging on individual loop failure
            logger.info(e)

            pass


async def redis_consumer(client: RedisClient) -> EventConsume:
    """Closure to provide redis client wh/ allows EventConsume to be used as value."""

    async def read_stream(consumer: Consumer) -> Tuple[Event, str] | None:
        """Reads an event stream configured via the given consumer."""

        stream = consumer.stream

        try:
            response = client.xread(
                {stream.value: consumer.acked_id},
                count=1,  # EVAL: Consider whether to do anything w/ count
            )

            if response:
                # We are only consuming a single stream
                redis_stream = response[0]
                _, messages = redis_stream

                for message_id, values in messages:
                    logger.info(
                        f"handling event from {stream}"
                        f"-- id: {message_id}"
                        f"-- type: {type(values)}"
                        f"-- event values: {values}\n"
                    )

                    # Hydrates the mapped event type w/ values from the stream
                    hydrated_event = STREAM_TO_EVENT_MAPPING[stream](
                        **json.loads(values["values"])
                    )

                    # Update latest read message.
                    # What's the best way to capture this, long term?
                    return hydrated_event, message_id

            return None

        except ConnectionError as e:
            logger.error(f"issue w/ redis connection on xread: {e}")

            raise Exception()

    return read_stream
