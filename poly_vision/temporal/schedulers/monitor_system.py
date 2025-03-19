import logging
import os
from datetime import timedelta

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
)

from poly_vision.temporal.workflows.indexer_workflow import (
    BlockchainIndexerWorkflow,
)


async def create_blockchain_scheduler(client):
    """Create blockchain indexer scheduler."""
    await client.create_schedule(
        id="blockchain-indexer-scheduler",
        schedule=Schedule(
            action=ScheduleActionStartWorkflow(
                BlockchainIndexerWorkflow.run,
                id="blockchain-indexer",
                task_queue="default",
                args=[10],  # batch size
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(minutes=1))]
                
            ),
            policy=SchedulePolicy(
                overlap=ScheduleOverlapPolicy.SKIP, catchup_window=timedelta(minutes=1)
            ),
        ),
    )


async def describe_schedule(client, schedule_id: str):
    """Describe a specific schedule."""
    handle = client.get_schedule_handle(schedule_id)
    desc = await handle.describe()
    logging.info("Schedule %s returns the note: %s", schedule_id)


async def monitor_system_scheduler():
    """Initialize and start all schedulers."""
    host = os.environ["TEMPORAL_HOST"]
    client = await Client.connect(host, namespace="default")

    # Setup blockchain indexer scheduler
    try:
        await describe_schedule(client, "blockchain-indexer-scheduler")
    except Exception:
        await create_blockchain_scheduler(client)
        logging.info("Created new blockchain indexer schedule")
