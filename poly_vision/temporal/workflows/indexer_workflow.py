from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
from typing import List
import asyncio

with workflow.unsafe.imports_passed_through():
    from poly_vision.utils.enums import IndexingStatus, IndexingResult
    from poly_vision.temporal.activities.indexer_activity import (
        index_block,
        get_latest_blocks,
    )


@workflow.defn
class BlockchainIndexerWorkflow:
    @workflow.run
    async def run(self, batch_size: int = 10):
        # Get latest block info
        latest_info = await workflow.execute_activity(
            get_latest_blocks,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        start_block = (
            latest_info["db_block"] + 1
            if latest_info["db_block"] is not None
            else latest_info["chain_block"]
        )
        chain_block = latest_info["chain_block"]

        total_processed = 0
        total_successful = 0

        while start_block <= chain_block:
            end_block = min(start_block + batch_size - 1, chain_block)

            # Create concurrent activities for each block in the range
            block_activities = []
            for block_num in range(start_block, end_block + 1):
                activity = workflow.execute_activity(
                    index_block,
                    args=[block_num],
                    schedule_to_close_timeout=timedelta(minutes=2),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    start_to_close_timeout=timedelta(minutes=1),
                )
                block_activities.append(activity)

            # Wait for all block activities to complete
            try:
                results = await asyncio.gather(*block_activities)

                # Process results - a result is successful if it's an integer (block number)
                successful_blocks = sum(1 for r in results if isinstance(r, int))

                total_processed += len(results)
                total_successful += successful_blocks

                # Log progress
                workflow.logger.info(
                    f"Processed blocks {start_block}-{end_block}: "
                    f"{successful_blocks}/{len(results)} successful"
                )

            except Exception as e:
                workflow.logger.error(
                    f"Error processing blocks {start_block}-{end_block}: {str(e)}"
                )

            # Update start_block for next batch
            start_block = end_block + 1

            # Add small delay between batches
            await asyncio.sleep(1)

        return {
            "status": "completed",
            "last_indexed_block": chain_block,
            "total_blocks_processed": total_processed,
            "successful_blocks": total_successful,
        }
