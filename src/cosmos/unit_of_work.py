from __future__ import annotations
from abc import ABC

from typing import Protocol, Type
from uuid import UUID

from cosmos.domain import Message
from cosmos.factory import Factory
from cosmos.repository import AggregateRepository


class TransactionalOutbox(Protocol):
    async def send(self, messages: list[Message], **kwargs):
        """Delievers a message to to transactional outbox"""

        pass


class ProcessedMessageRepository(Protocol):
    """..."""

    async def is_processed(self, message_id: UUID) -> bool: ...

    async def mark_processed(self, message_id: UUID): ...


class UnitOfWork(ABC):
    """A class dedicated to defining what one 'unit' of work is.

    This is an implementation of the unit of work pattern. Its core
    responsibility is providing a single context for a repository to
    share. If any repository action fails, all previous changes up to
    that point shall be rolled-back.
    """

    repository: AggregateRepository
    outbox: TransactionalOutbox
    processed_message_repository: ProcessedMessageRepository

    def __init__(
        self,
        repository: AggregateRepository,
        outbox: TransactionalOutbox,
        processed_message_repository: ProcessedMessageRepository,
    ):
        self.repository = repository
        self.outbox = outbox
        self.processed_messages = processed_message_repository

    async def __aenter__(self) -> UnitOfWork:
        raise NotImplementedError

    async def __aexit__(self, *args):
        raise NotImplementedError


class UnitOfWorkFactory(Factory):
    def __init__(
        self: "UnitOfWorkFactory",
        unit_of_work_cls: Type[UnitOfWork],
        **uow_kwargs,
    ):
        self._uow_cls = unit_of_work_cls
        self._uow_kwargs = uow_kwargs

    def get(self: "UnitOfWorkFactory") -> UnitOfWork:
        kwargs = {
            k: v.get() if isinstance(v, Factory) else v
            for k, v in self._uow_kwargs.items()
        }

        return self._uow_cls(**kwargs)
