from typing import List

import pytest

from domain.model import Inspection
from microservices.domain import Event
from tests.conftest import MockInspectionRepository, MockUnitOfWork


@pytest.mark.asyncio
async def test_uow(mock_inspection_list: List[Inspection]):
    repo = MockInspectionRepository(inspections=mock_inspection_list)
    uow = MockUnitOfWork(repository=repo)

    async with uow:
        assert list(uow.collect_events()) == []

        agg = await uow.repository.get(id=1)
        agg.new_event(Event())
        events = uow.collect_events()

        assert len(list(events)) == 1
        assert len(list(uow.collect_events())) == 0

        agg.new_event(Event())
        agg.new_event(Event())

        events = uow.collect_events()

        assert len(list(events)) == 2
        assert len(list(uow.collect_events())) == 0
