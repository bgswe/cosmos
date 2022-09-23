from typing import List
from uuid import UUID

from tortoise import fields

from microservices.contrib.tortoise.models import AbstractBaseModel
from microservices.domain import PK, Consumer
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
                pk=UUID(c.pk),
                stream=c.stream,
                name=c.name,
                acked_id=c.acked_id,
                retroactive=c.retroactive,
            )
            for c in consumer_orm_list
        ]

    async def _get(self, pk: PK) -> Consumer:
        consumer_orm = await ConsumerORM.get(pk=pk)

        return Consumer(
            pk=UUID(consumer_orm.pk),
            stream=consumer_orm.stream,
            name=consumer_orm.name,
            acked_id=consumer_orm.acked_id,
            retroactive=consumer_orm.retroactive,
        )

    async def _add(self, consumer: Consumer):
        await ConsumerORM.create(
            pk=consumer.pk,
            stream=consumer.stream,
            name=consumer.name,
            acked_id=consumer.acked_id,
            retroactive=consumer.retroactive,
        )

    async def _update(self, consumer: Consumer):
        consumer_orm = await ConsumerORM.get(pk=consumer.pk)

        # TODO: Only updating acked_id for now
        consumer_orm.acked_id = consumer.acked_id

        await consumer_orm.save()
