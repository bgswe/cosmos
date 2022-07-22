from typing import List

from tortoise import fields

from microservices.contrib.tortoise import AbstractBaseModel
from microservices.domain import EventStreams, Subscription
from microservices.repository import AsyncRepository


class SubscriptionORM(AbstractBaseModel):
    stream = fields.CharEnumField(EventStreams)
    webhook_url = fields.CharField(max_length=255)
    health_check_url = fields.CharField(max_length=255)

    class Meta:
        table = "subscription"


class TortoiseSubscriptionRepository(AsyncRepository):
    async def _get_list(self) -> List[Subscription]:
        sub_orms = await SubscriptionORM.all()

        sub_list = [
            Subscription(
                id=s.id,
                stream=s.stream,
                webhook_url=s.webhook_url,
                health_check_url=s.health_check_url,
            )
            for s in sub_orms
        ]

        return sub_list

    async def _get():
        pass

    async def _add(self, sub: Subscription):
        await SubscriptionORM.create(
            id=sub.id,
            stream=sub.stream,
            webhook_url=sub.webhook_url,
            health_check_url=sub.health_check_url,
        )

    async def _update():
        pass
