from tortoise import fields

from microservices.contrib.tortoise import AbstractBaseModel
from microservices.domain import EventStreams


class SubscriptionORM(AbstractBaseModel):
    stream = fields.CharEnumField(EventStreams)
    webhook_url = fields.CharField(max_length=255)
    health_check_url = fields.CharField(max_length=255)

    class Meta:
        table = "subscription"
