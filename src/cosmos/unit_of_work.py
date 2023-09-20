from __future__ import annotations
from abc import ABC

from typing import (
    List,
    Protocol,
)
from uuid import UUID

from cosmos.domain import Message
from cosmos.repository import AggregateRepository


class TransactionalOutbox(Protocol):
    async def send(self, messages: List[Message], **kwargs):
        """Delievers a message to to transactional outbox"""

        pass


class ProcessedMessageRepository(Protocol):
    """..."""

    def is_processed(self, message_id: UUID) -> bool:
        ...

    def mark_processed(self, message_id: UUID):
        ...


class UnitOfWork(ABC):
    """A class dedicated to defining what one 'unit' of work is.

    This is an implementation of the unit of work pattern. Its core
    responsibility is providing a single context for a repository to
    share. If any repository action fails, all previous changes up to
    that point shall be reverted.
    """

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
