from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from typing import Dict, Any

with workflow.unsafe.imports_passed_through():
    from poly_vision.utils.enums import IndexingStatus
    from poly_vision.temporal.activities.indexer_activity import (
        index_block_range,
        get_latest_blocks,
    )


@workflow.defn
class BlockchainIndexerWorkflow:
    @workflow.run
    async def run(self, batch_size: int = 10):
        latest_info = await workflow.execute_activity(
            get_latest_blocks,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        start_block = (
            latest_info["db_block"] + 1
            if latest_info["db_block"]
            else latest_info["chain_block"]
        )
        chain_block = latest_info["chain_block"]

        while start_block <= chain_block:
            end_block = min(start_block + batch_size - 1, chain_block)

            result = await workflow.execute_activity(
                index_block_range,
                args=[start_block, end_block],
                schedule_to_close_timeout=timedelta(minutes=5),
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            return result

            if result["status"] == IndexingStatus.COMPLETED:
                start_block = end_block + 1
                if start_block % (batch_size * 10) == 0 or start_block > chain_block:
                    latest_info = await workflow.execute_activity(
                        get_latest_blocks,
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                    chain_block = latest_info["chain_block"]

            await workflow.sleep(timedelta(seconds=1))

        return {"status": "completed", "last_indexed_block": start_block - 1}
