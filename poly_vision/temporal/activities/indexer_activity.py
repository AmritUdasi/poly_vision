from temporalio import activity
from typing import Dict, Any, Optional, List
from poly_vision.services.blockchain_service import BlockchainService
from poly_vision.services.database_service import DatabaseService
from poly_vision.utils.config import load_config
from poly_vision.utils.enums import (
    BlockRangeResult,
    IndexingResult,
    IndexingStatus,
    IndexingErrorType,
    BlockData,
    Transaction,
)
import asyncio
from datetime import datetime

# Load config once at module level
config = load_config()
blockchain_service = BlockchainService(config.blockchain)
db_service = DatabaseService(config.database)


@activity.defn
async def index_block(block_number: int):
    """Index a specific block from Polygon network."""
    try:
        # Connect to database
        await db_service.connect()

        try:
            # Get block with transactions using blockchain service
            block = await blockchain_service.get_block_with_transactions(block_number)

            # Create block data with all fields
            block_data = BlockData(
                block_number=block.number,
                timestamp=block.timestamp,
                hash=block.hash.hex(),
                parent_hash=block.parentHash.hex(),
                nonce=block.nonce.hex(),
                sha3_uncles=(
                    block.sha3Uncles.hex() if hasattr(block, "sha3Uncles") else None
                ),
                logs_bloom=(
                    block.logsBloom.hex() if hasattr(block, "logsBloom") else None
                ),
                transactions_root=block.transactionsRoot.hex(),
                state_root=block.stateRoot.hex(),
                receipts_root=block.receiptsRoot.hex(),
                miner=block.miner,
                difficulty=0,
                total_difficulty=0,
                size=block.size,
                extra_data=(
                    block.extraData.hex() if hasattr(block, "extraData") else None
                ),  # Make extraData optional
                gas_limit=block.gasLimit,
                gas_used=block.gasUsed,
                transaction_count=len(block.transactions),
                base_fee_per_gas=(
                    block.baseFeePerGas if hasattr(block, "baseFeePerGas") else None
                ),
            )

            # Save block
            await db_service.save_block(block_data)

            # Process transactions and traces
            for tx in block.transactions:
                tx_dict = dict(tx)
                tx_hash = tx.hash.hex()

                try:
                    traces = blockchain_service.w3.provider.make_request(
                        "debug_traceTransaction", [tx_hash, {"tracer": "callTracer"}]
                    )

                    # Create transaction data first
                    transaction_data = Transaction(
                        hash=tx_hash,
                        blockHash=tx_dict["blockHash"].hex(),
                        blockNumber=tx_dict["blockNumber"],
                        from_=tx_dict["from"],
                        to=tx_dict["to"],
                        gas=tx_dict["gas"],
                        gasPrice=tx_dict["gasPrice"],
                        maxFeePerGas=tx_dict.get("maxFeePerGas"),
                        maxPriorityFeePerGas=tx_dict.get("maxPriorityFeePerGas"),
                        input=(
                            tx_dict["input"].hex()
                            if isinstance(tx_dict["input"], bytes)
                            else tx_dict["input"]
                        ),
                        nonce=tx_dict["nonce"],
                        value=tx_dict["value"],
                        type=tx_dict["type"],
                        chainId=tx_dict["chainId"],
                        v=tx_dict["v"],
                        r=tx_dict["r"].hex(),
                        s=tx_dict["s"].hex(),
                        yParity=tx_dict.get("yParity"),
                        gas_used=tx_dict["gas"],
                        status=1,
                        traces=traces.get("result", {}),
                    )

                    # Save transaction
                    await db_service.save_transaction(transaction_data)

                except Exception as e:
                    print(f"Error processing transaction {tx_hash}: {str(e)}")
                    continue

            return block_number

        except Exception as e:
            raise Exception(f"Failed to process block {block_number}: {str(e)}")
        finally:
            await db_service.disconnect()

    except Exception as e:
        raise Exception(f"Failed to index block {block_number}: {str(e)}")


@activity.defn
async def index_block_range(
    start_block: int,
    end_block: int,
    batch_size: Optional[int] = None,
    max_concurrent: int = 5,
) -> BlockRangeResult:
    """Index a range of blocks from Polygon network."""
    if start_block > end_block:
        raise ValueError("start_block must be less than or equal to end_block")

    batch_size = batch_size or config.batch_size
    results: List[IndexingResult] = []
    error_counts: Dict[IndexingErrorType, int] = {}

    try:
        for block_number in range(start_block, end_block + 1, batch_size):
            batch_end = min(block_number + batch_size, end_block + 1)
            batch_tasks = [
                index_block(block_num) for block_num in range(block_number, batch_end)
            ]

            # Process blocks in smaller concurrent batches
            for i in range(0, len(batch_tasks), max_concurrent):
                concurrent_batch = batch_tasks[i : i + max_concurrent]
                try:
                    batch_results = await asyncio.gather(
                        *concurrent_batch, return_exceptions=True
                    )

                    # Process results and handle exceptions
                    for result in batch_results:
                        if isinstance(result, Exception):
                            error_result = IndexingResult(
                                status=IndexingStatus.ERROR,
                                block_number=-1,
                                transactions_indexed=0,
                                error_type=IndexingErrorType.UNKNOWN_ERROR,
                                error_message=str(result),
                                timestamp=datetime.utcnow(),
                            )
                            results.append(error_result)
                        else:
                            results.append(result)
                            if result.error_type:
                                error_counts[result.error_type] = (
                                    error_counts.get(result.error_type, 0) + 1
                                )

                except Exception as e:
                    print(f"Error processing batch: {str(e)}")
                    continue

                await asyncio.sleep(0.1)

        successful_blocks = sum(
            1 for r in results if r.status == IndexingStatus.SUCCESS
        )

        failed_blocks = sum(1 for r in results if r.status == IndexingStatus.ERROR)

        return BlockRangeResult(
            status=IndexingStatus.COMPLETED,
            blocks_processed=len(results),
            blocks_successful=successful_blocks,
            blocks_failed=failed_blocks,
            start_block=start_block,
            end_block=end_block,
            results=results,
            error_counts=error_counts,
        )

    except Exception as e:
        return BlockRangeResult(
            status=IndexingStatus.ERROR,
            blocks_processed=len(results),
            blocks_successful=sum(
                1 for r in results if r.status == IndexingStatus.SUCCESS
            ),
            blocks_failed=sum(1 for r in results if r.status == IndexingStatus.ERROR),
            start_block=start_block,
            end_block=end_block,
            results=results,
            error_counts=error_counts,
        )


@activity.defn
async def get_latest_blocks() -> Dict[str, Any]:
    """Get latest block numbers from both DB and blockchain."""
    try:
        # Connect to database
        await db_service.connect()

        try:
            # Get latest block from DB
            latest_db_number = await db_service.get_latest_block_number()

            # Get latest block from chain
            latest_chain_block = await blockchain_service.get_latest_block_number()

            return {"db_block": latest_db_number, "chain_block": latest_chain_block}
        finally:
            await db_service.disconnect()

    except Exception as e:
        raise Exception(f"Failed to get latest blocks: {str(e)}")


def process_traces(
    trace: Dict[str, Any],
    tx_hash: str,
    block_number: int,
    block_hash: str,
    block_timestamp: int,
    parent_address: str = "",
) -> List[Dict[str, Any]]:
    """Process traces recursively and prepare them for saving."""
    traces_to_save = []

    def process_single_trace(trace: Dict[str, Any], trace_address: str = "") -> Dict:
        return {
            "transaction_hash": tx_hash,
            "transaction_index": 0,
            "from_address": trace.get("from"),
            "to_address": trace.get("to"),
            "value": trace.get("value", "0"),
            "input": trace.get("input"),
            "output": trace.get("output"),
            "trace_type": trace.get("type", "call"),
            "call_type": trace.get("callType"),
            "reward_type": None,
            "gas": (
                int(trace.get("gas", "0"), 16)
                if isinstance(trace.get("gas"), str)
                else trace.get("gas", 0)
            ),
            "gas_used": (
                int(trace.get("gasUsed", "0"), 16)
                if isinstance(trace.get("gasUsed"), str)
                else trace.get("gasUsed", 0)
            ),
            "subtraces": len(trace.get("calls", [])),
            "trace_address": trace_address,
            "error": trace.get("error"),
            "status": 1 if not trace.get("error") else 0,
            "trace_id": f"{tx_hash}-{trace_address}" if trace_address else tx_hash,
            "block_number": block_number,
            "block_hash": block_hash,
            "block_timestamp": datetime.fromtimestamp(block_timestamp),
        }

    # Process main trace
    traces_to_save.append(process_single_trace(trace, parent_address))

    # Process nested calls
    for i, call in enumerate(trace.get("calls", [])):
        trace_address = f"{parent_address}-{i}" if parent_address else str(i)
        nested_traces = process_traces(
            call, tx_hash, block_number, block_hash, block_timestamp, trace_address
        )
        traces_to_save.extend(nested_traces)

    return traces_to_save
