from typing import List
from uuid import UUID

from tortoise import fields

from cosmos.contrib.tortoise.models import AbstractBaseModel
from cosmos.domain import Consumer
from cosmos.repository import AsyncRepository


class ConsumerORM(AbstractBaseModel):
    stream = fields.CharField(max_length=255)
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
                pk=UUID(c.pk),
                stream=c.stream,
                name=c.name,
                acked_id=c.acked_id,
                retroactive=c.retroactive,
            )
            for c in consumer_orm_list
        ]

    async def _get(self, id: UUID) -> Consumer:
        consumer_orm = await ConsumerORM.get(pk=id)

        return Consumer(
            pk=UUID(consumer_orm.pk),
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
        consumer_orm = await ConsumerORM.get(pk=consumer.id)

        # TODO: Only updating acked_id for now
        consumer_orm.acked_id = consumer.acked_id

        await consumer_orm.save()