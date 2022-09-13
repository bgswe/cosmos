from typing import List
from uuid import UUID

from tortoise import fields

from microservices.contrib.tortoise.models import AbstractBaseModel
from microservices.domain import Consumer
from microservices.events import EventStream
from microservices.repository import AsyncRepository


class ConsumerORM(AbstractBaseModel):
    stream = fields.CharEnumField(EventStream)
    name = fields.CharField(max_length=255)
    acked_id = fields.CharField(max_length=255)
    retroactive = fields.BooleanField()

    class Meta:
        table = "consumer"


class TortoiseConsumerRepository(AsyncRepository[Consumer]):
    async def _get_list(self, **kwargs) -> List[Consumer]:
        consumer_orm_list = await ConsumerORM.all()

        return [
            Consumer(
                id=c.id,
                stream=c.stream,
                name=c.name,
                acked_id=c.acked_id,
                retroactive=c.retroactive,
            )
            for c in consumer_orm_list
        ]

    async def _get(self, id: UUID) -> Consumer:
        consumer_orm = await ConsumerORM.get(id=id)

        return Consumer(
            id=consumer_orm.id,
            stream=consumer_orm.stream,
            name=consumer_orm.name,
            acked_id=consumer_orm.acked_id,
            retroactive=consumer_orm.retroactive,
        )

    async def _add(self, consumer: Consumer):
        await ConsumerORM.create(
            id=consumer.id,
            stream=consumer.stream,
            name=consumer.name,
            acked_id=consumer.acked_id,
            retroactive=consumer.retroactive,
        )

    async def _update(self, consumer: Consumer):
        consumer_orm = await ConsumerORM.get(id=consumer.id)

        # TODO: Only updating acked_id for now
        consumer_orm.acked_id = consumer.acked_id

        await consumer_orm.save()
