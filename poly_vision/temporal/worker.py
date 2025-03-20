import asyncio
import time

from poly_vision.services.database_service import DatabaseService
from poly_vision.temporal.activities.indexer_activities import (
    get_latest_blocks,
    index_block,
    index_block_range,
)
from poly_vision.temporal.schedulers.monitor_system import monitor_system_scheduler
from poly_vision.temporal.workflows.indexer_workflow import BlockchainIndexerWorkflow
from temporalio.client import Client
from temporalio.worker import Worker
from poly_vision.configs.config import settings
from poly_vision.utils.config import load_config


async def start_temporal_worker(worker_names):
    config = load_config()
    db_service = DatabaseService(config.database)
    await db_service.prisma.connect()
    print(settings.TEMPORAL_HOST)
    client = await Client.connect(settings.TEMPORAL_HOST, namespace="default")
    await monitor_system_scheduler()

    workers = {
        "poly_vision": Worker(
            client,
            task_queue="default",
            workflows=[
                BlockchainIndexerWorkflow,
            ],
            activities=[get_latest_blocks, index_block_range, index_block],
            max_concurrent_workflow_task_polls=13,
            max_concurrent_activity_task_polls=13,
        ),
    }

    tasks = []
    for worker_name in worker_names:
        worker = workers.get(worker_name)
        if worker:
            time.sleep(10)
            tasks.append(worker.run())

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    temporal_workers = settings.TEMPORAL_WORKERS.split(",")
    asyncio.run(start_temporal_worker(temporal_workers))
