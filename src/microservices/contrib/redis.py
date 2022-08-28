import asyncio
import json
import logging
import os
from typing import Any, Callable, Dict, List, Protocol
from redis import Redis

from microservices.domain import Consumer
from microservices.events import (
    STREAM_TO_EVENT_MAPPING,
    ConsumerConfig,
    Domain,
    Event,
    EventStream,
)
from microservices.message_bus import MessageBus
from microservices.unit_of_work import AsyncUnitOfWork, AsyncUOWFactory


class RedisClient(Protocol):
    """Mirror for used methods from redis-py to easily mock a redis client."""

    def xadd(name, fields, id='*', maxlen=None, approximate=True, nomkstream=False, minid=None, limit=None):
        ...

class RedisPublisher:
    def __init__(self, redis: RedisClient):
        self._redis = redis

    async def publish(self, event: Event):
        """..."""

        values = event.dict()

        try:
            response = self._redis.xadd(
                name=event.stream.value,
                fields={"values": json.dumps({values})},
            )

            logging.debug(f"redis xadd command response: {response}")

        except Exception as e:
            logging.debug(f"redis xadd failed with exception type: {type(e)}")
            logging.debug(f"-- event: f{values}")
            logging.debug(f"-- exception: f{e}")

            # TODO: What todo when message publish fails?


class RedisConsumer:
    pass


def get_redis_client() -> Redis:
    """Encapsulated configuration management for redis-py client."""

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

    # Establish Redis client connection.
    return Redis(
        host=REDIS_HOST, port=REDIS_PORT, retry_on_timeout=True, decode_responses=True
    )


async def read_stream(
    domain: Domain,
    uow_factory: AsyncUOWFactory,
    client: Redis,
    consumer: Consumer,
    target: Callable[[AsyncUnitOfWork, Dict[str, Any]], None],
    sleep: int = 3,
):
    """..."""

    stream = consumer.stream

    target_name = target.__name__

    logging.info(
        f"now reading stream {stream} with target {target_name}"
        f"with acked_id {consumer.acked_id}"
    )

    bus = MessageBus(
        domain=domain,
        uow_factory=uow_factory,
        event_handlers={stream: [target]},
        publisher=RedisPublisher(client=get_redis_client()),
    )

    while True:
        try:
            async with uow_factory.get_uow() as uow:
                consumer = await uow.repository.get(id=consumer.id)

                response = client.xread(
                    {stream.value: consumer.acked_id},
                    count=1,
                )

                if response:
                    # We are only consuming a single stream
                    redis_stream = response[0]
                    _, messages = redis_stream

                    for message_id, values in messages:
                        logging.info(
                            f"handling event from {stream}"
                            f"-- id: {message_id}"
                            f"-- target_name: {target_name}"
                            f"-- type: {type(values)}"
                            f"-- event values: {values}\n"
                        )

                        # Hydrates the mapped domain event view values from the stream
                        hydrated_event = STREAM_TO_EVENT_MAPPING[stream](
                            **json.loads(values["values"])
                        )

                        await bus.handle(message=hydrated_event)

                        # Update latest read message.
                        # What's the best way to capture this, long term?
                        consumer.acked_id = message_id

                        await uow.repository.update(consumer)

                        logging.info(f"-- handled {stream} message w/ id {message_id}")

                await asyncio.sleep(sleep)

        except ConnectionError as e:
            logging.error(f"issue w/ redis connection on xread: {e}")


async def redis_consumer(
    loop,
    domain: Domain,
    config: Dict[EventStream, List[ConsumerConfig]],
    uow_factory: AsyncUOWFactory,
):
    """Consumes events defined in handlers from redis streams.

    :param: handlers -> EventStreams of interest, and the associated handlers.
    :param: subscriptions -> An implementation of the subs repo.
    """
    client = get_redis_client()

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

    async with uow_factory.get_uow() as uow:
        for c in created_consumers:
            if not c.retroactive:
                # TODO: Get the ~latest ID, and set the acked_id to that
                # to prevent it from processing a ton of old messages
                pass

            await uow.repository.add(c)

    # Get all consumers from repo, and create a read_stream coroutine
    async with uow_factory.get_uow() as uow:
        consumers = await uow.repository.get_list()

    # Map config to Consumer, need to align consumer target
    flattened_configs = [
        config for config_list in config.values() for config in config_list
    ]
    ccs = [
        (c, next(config for config in flattened_configs if c.name == config.name))
        for c in consumers
    ]

    async_tasks = [
        loop.create_task(
            read_stream(
                domain=domain,
                uow_factory=uow_factory,
                client=client,
                consumer=consumer,
                target=config.target,
            )
        )
        for consumer, config in ccs
    ]

    await asyncio.wait(async_tasks)
