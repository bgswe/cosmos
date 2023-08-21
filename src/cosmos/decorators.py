from datetime import datetime as dt
from typing import Type

from cosmos.unit_of_work import AsyncUnitOfWork
from cosmos.domain import CommandComplete, CommandCompletionStatus


def handler_decorator_factory(
    AggregateRootClass: Type,
    is_command: bool = False,  # will create completion event if True
):
    def handler_decorator(handler_func):  # actual decorator func
        # function which is invoked in place of decorated func
        async def inner_func(uow: AsyncUnitOfWork, *args, **kwargs):
            # enable ability to run ancillary code after command handled, under single transaction
            async with uow.context(AggregateRootClass=AggregateRootClass) as uow:
                # invoke decorated business logic with given command
                await handler_func(uow=uow, *args, **kwargs)

                events = []
                for seen in uow.repository.seen:
                    events += seen.events

                if is_command:
                    command = kwargs["command"]

                    completion_event = CommandComplete(
                        command_id=command.message_id,
                        command_name=command.name,
                        timestamp=dt.now(),
                        status=CommandCompletionStatus.SUCCESS,
                    )

                    events.append(completion_event)

                # TODO: implement iterating through the events and inserting them into the
                # TODO: outbox table
                print(events)

        return inner_func

    return handler_decorator
