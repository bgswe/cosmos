from cosmos.repository import AggregateReplay, EventHydrator
from cosmos.contrib.pg import (
    PostgresOutbox,
    PostgresEventStore,
    PostgresUnitOfWork,
    PostgresProcessedMessageRepository,
)
from dependency_injector import containers, providers

from utils import generate_postgres_pool


class PostgresDomainContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    connection_pool = providers.Resource(
        generate_postgres_pool,
        database=config.database_name,
        user=config.database_user,
    )

    event_hydrator = providers.Factory(
        EventHydrator,
        aggregate_root_mapping=config.aggregate_root_mapping,
        event_hydration_mapping=config.event_hydration_mapping,
    )

    replay_handler = providers.Factory(
        AggregateReplay,
        event_hydrator=event_hydrator,
    )

    repository = providers.Factory(
        PostgresEventStore,
        replay_handler=replay_handler,
    )

    outbox = providers.Factory(
        PostgresOutbox,
    )

    processed_messages = providers.Factory(
        PostgresProcessedMessageRepository,
    )

    unit_of_work = providers.Factory(
        PostgresUnitOfWork,
        processed_message_repository=processed_messages,
        pool=connection_pool,
        repository=repository,
        outbox=outbox,
    )
