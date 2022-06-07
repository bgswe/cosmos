from tortoise import fields, Model


class TimestampMixin:
    created_at = fields.DatetimeField(null=True, auto_now_add=True)
    modified_at = fields.DatetimeField(null=True, auto_now=True)


class BaseMixin:
    # TODO: Validate this to be UUIDv4
    id = fields.CharField(max_length=36, pk=True, generated=False)  # UUIDv4


class AbstractBaseModel(Model, BaseMixin, TimestampMixin):
    class Meta:
        abstract = True
