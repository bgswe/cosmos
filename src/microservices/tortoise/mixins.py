from tortoise import fields


class TimestampMixin:
    created_at = fields.DatetimeField(null=True, auto_now_add=True)
    modified_at = fields.DatetimeField(null=True, auto_now=True)


class BaseMixin:
    id = fields.CharField(max_length=36, pk=True, generated=False)  # UUIDv4
