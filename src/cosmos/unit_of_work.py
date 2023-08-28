from __future__ import annotations
from abc import ABC

from typing import (
    List,
    Protocol,
)

from cosmos.domain import Message
from cosmos.repository import AggregateRepository


class TransactionalOutbox(Protocol):
    async def send(self, messages: List[Message]):
        """Delievers a message to to transactional outbox"""

        pass


class UnitOfWork(ABC):
    """A class dedicated to defining what one 'unit' of work is.

    This is an implementation of the unit of work pattern. Its core
    responsibility is providing a single context for a repository to
    share. If any repository action fails, all previous changes up to
    that point shall be reverted.
    """

    repository: AggregateRepository
    outbox: TransactionalOutbox

    async def __aenter__(self) -> UnitOfWork:
        raise NotImplementedError

    async def __aexit__(self, *args):
        raise NotImplementedError

    async def send_events_to_outbox(self):
        events = [event for agg in self.repository.seen for event in agg.events]

        await self.outbox.send(messages=events)
