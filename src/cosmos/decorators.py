from datetime import datetime as dt

from cosmos.unit_of_work import UnitOfWork
from cosmos.domain import Command, CommandComplete, CommandCompletionStatus


def command(handler_func):  # actual decorator func
    # function which is invoked in place of decorated func
    async def inner_func(uow: UnitOfWork, command: Command):
        # enable ability to run ancillary code after command handled, under single transaction
        async with uow as uow:
            # invoke decorated business logic with given command
            await handler_func(uow=uow, command=Command)

        completion_event = CommandComplete(
            command_id=command.message_id,
            command_name=command.name,
            timestamp=dt.now(),
            status=CommandCompletionStatus.SUCCESS,
        )

        await uow.outbox.send(messages=[completion_event])

    return inner_func
