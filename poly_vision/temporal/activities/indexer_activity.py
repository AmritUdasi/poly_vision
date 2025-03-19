from temporalio import activity
from typing import Dict, Any, Optional, List
from poly_vision.services.blockchain_service import BlockchainService
from poly_vision.services.database_service import DatabaseService
from poly_vision.utils.config import load_config
from poly_vision.utils.enums import (
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
            ),
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

                # Create transaction data
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
        raise Exception(f"Failed to index block {block_number}: {str(e)}")


@activity.defn
async def index_block_range(
    start_block: int,
    end_block: int,
    batch_size: Optional[int] = None,
    max_concurrent: int = 5,
):
    """Index a range of blocks from Polygon network."""
    if start_block > end_block:
        raise ValueError("start_block must be less than or equal to end_block")

    results = {
        "blocks_processed": 0,
        "blocks_successful": 0,
        "blocks_failed": 0,
        "failed_blocks": []
    }

    try:
        # Connect to database once for the entire range
        await db_service.connect()

        try:
            current_block = start_block
            while current_block <= end_block:
                # Create batch of concurrent tasks
                batch_tasks = []
                for _ in range(max_concurrent):
                    if current_block <= end_block:
                        batch_tasks.append(index_block(current_block))
                        current_block += 1

                if not batch_tasks:
                    break

                # Process the concurrent batch
                batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                results["blocks_processed"] += len(batch_results)

                # Process results
                for block_num, result in zip(
                    range(current_block - len(batch_tasks), current_block), 
                    batch_results
                ):
                    if isinstance(result, Exception):
                        results["blocks_failed"] += 1
                        results["failed_blocks"].append({
                            "block": block_num,
                            "error": str(result)
                        })
                        activity.logger.warning(
                            f"Failed to process block {block_num}: {str(result)}"
                        )
                    else:
                        results["blocks_successful"] += 1
                        activity.logger.info(f"Successfully processed block {block_num}")

                await asyncio.sleep(0.1)  # Small delay between batches

            return results

        finally:
            # Always disconnect when done
            await db_service.disconnect()

    except Exception as e:
        activity.logger.error(f"Error in index_block_range: {str(e)}")
        raise e


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
