from datetime import timedelta
from temporalio import workflow
from temporalio.common import RetryPolicy
import asyncio

with workflow.unsafe.imports_passed_through():
    from poly_vision.temporal.activities.indexer_activities import (
        index_block_range,
        get_latest_blocks,
    )


@workflow.defn
class BlockchainIndexerWorkflow:
    @workflow.run
    async def run(self, batch_size: int = 10, max_concurrent: int = 10):
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
        failed_blocks = []

        while start_block <= chain_block:
            end_block = min(start_block + batch_size - 1, chain_block)

            try:
                # Process a range of blocks
                range_result = await workflow.execute_activity(
                    index_block_range,
                    args=[start_block, end_block],
                    schedule_to_close_timeout=timedelta(minutes=5),
                    retry_policy=RetryPolicy(maximum_attempts=2),
                    start_to_close_timeout=timedelta(minutes=3),
                )

                # Update statistics from the result
                total_processed += range_result["blocks_processed"]
                total_successful += range_result["blocks_successful"]

                if range_result["failed_blocks"]:
                    failed_blocks.extend(range_result["failed_blocks"])
                    workflow.logger.warn(
                        f"Failed blocks in range {start_block}-{end_block}: {range_result['failed_blocks']}"
                    )

                # Log progress
                workflow.logger.info(
                    f"Processed blocks {start_block}-{end_block}: "
                    f"{range_result['blocks_successful']}/{range_result['blocks_processed']} successful"
                )

            except Exception as e:
                workflow.logger.error(
                    f"Failed to process range {start_block}-{end_block}: {str(e)}"
                )
                failed_blocks.append(
                    {"block_range": [start_block, end_block], "error": str(e)}
                )

            # Update start_block for next batch
            start_block = end_block + 1

            # Add small delay between batches
            await asyncio.sleep(0.5)

        return {
            "status": "completed",
            "last_indexed_block": chain_block,
            "total_blocks_processed": total_processed,
            "successful_blocks": total_successful,
            "failed_blocks": failed_blocks,
        }
