from tortoise import Model

from .mixins import BaseMixin, TimestampMixin


class AbstractBaseModel(Model, BaseMixin, TimestampMixin):
    class Meta:
        abstract = True
