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
        await db_service.connect()

        if await db_service.block_exists(block_number):
            activity.logger.info(f"Block {block_number} already exists, skipping...")
            return block_number

        block = await blockchain_service.get_block_with_transactions(block_number)

        # Save block data
        block_data = BlockData(
            block_number=block.number,
            timestamp=block.timestamp,
            hash=block.hash.hex(),
            parent_hash=block.parentHash.hex(),
            nonce=block.nonce.hex(),
            sha3_uncles=(
                block.sha3Uncles.hex() if hasattr(block, "sha3Uncles") else None
            ),
            logs_bloom=block.logsBloom.hex() if hasattr(block, "logsBloom") else None,
            transactions_root=block.transactionsRoot.hex(),
            state_root=block.stateRoot.hex(),
            receipts_root=block.receiptsRoot.hex(),
            miner=block.miner,
            difficulty=0,
            total_difficulty=0,
            size=block.size,
            extra_data=block.extraData.hex() if hasattr(block, "extraData") else None,
            gas_limit=block.gasLimit,
            gas_used=block.gasUsed,
            transaction_count=len(block.transactions),
            base_fee_per_gas=(
                block.baseFeePerGas if hasattr(block, "baseFeePerGas") else None
            ),
        )
        await db_service.save_block(block_data)

        # Get all traces for the block in a single RPC call
        trace_result =  blockchain_service.w3.provider.make_request(
            "debug_traceBlockByNumber", [hex(block.number), {"tracer": "callTracer"}]
        )

        transactions_batch = []

        # Process transactions
        for tx in block.transactions:
            tx_dict = dict(tx)
            transaction_data = Transaction(
                hash=tx.hash.hex(),
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
            )
            transactions_batch.append(transaction_data)

        # Parse all traces including internal ones
        traces_batch = await parse_traces(
            trace_result.get("result", []),
            block_number,
            block.hash.hex(),
            block.timestamp,
        )

        # Save batches concurrently
        if transactions_batch:
            try:
                await db_service.save_transactions_batch(transactions_batch)
                print(
                    f"Block {block_number}: Saved {len(transactions_batch)} transactions"
                )
            except Exception as e:
                print(f"Failed to save transactions for block {block_number}: {str(e)}")

        if traces_batch:
            try:
                await db_service.save_traces_batch(traces_batch)
                print(f"Block {block_number}: Saved {len(traces_batch)} traces")
            except Exception as e:
                print(f"Failed to save traces for block {block_number}: {str(e)}")

        await db_service.disconnect()
        return block_number

    except Exception as e:
        await db_service.disconnect()
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
        "blocks_skipped": 0,
        "failed_blocks": [],
    }

    try:
        await db_service.connect()

        try:
            current_block = start_block
            while current_block <= end_block:
                # Check which blocks in the next batch already exist
                batch_blocks = range(
                    current_block, min(current_block + max_concurrent, end_block + 1)
                )
                batch_tasks = []

                for block_num in batch_blocks:
                    # if await db_service.block_exists(block_num):
                    #     results["blocks_skipped"] += 1
                    #     activity.logger.info(f"Block {block_num} already exists, skipping...")
                    #     continue
                    batch_tasks.append(index_block(block_num))

                if not batch_tasks:
                    current_block += max_concurrent
                    continue

                # Process the concurrent batch
                batch_results = await asyncio.gather(
                    *batch_tasks, return_exceptions=True
                )
                results["blocks_processed"] += len(batch_results)

                # Process results
                for block_num, result in zip([b for b in batch_blocks], batch_results):
                    if isinstance(result, Exception):
                        results["blocks_failed"] += 1
                        results["failed_blocks"].append(
                            {"block": block_num, "error": str(result)}
                        )
                        activity.logger.warning(
                            f"Failed to process block {block_num}: {str(result)}"
                        )
                    else:
                        results["blocks_successful"] += 1
                        activity.logger.info(
                            f"Successfully processed block {block_num}"
                        )

                current_block += max_concurrent
                await asyncio.sleep(0.1)  # Small delay between batches

            return results

        finally:
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


async def parse_traces(
    trace_result: List[Dict], block_number: int, block_hash: str, block_timestamp: int
) -> List[Dict]:
    """
    Parse Ethereum debug_traceBlockByNumber results to extract all traces including internal ones.

    Args:
        trace_result: List of transaction traces from debug_traceBlockByNumber
        block_number: Block number
        block_hash: Block hash
        block_timestamp: Block timestamp

    Returns:
        List of traces formatted according to our schema
    """
    all_traces = []
    parse_tasks = []

    for tx in trace_result:
        tx_hash = tx.get("txHash")
        if not tx.get("result"):  # Skip if no result
            continue

        # Add main trace
        main_trace = {
            "transaction_hash": tx_hash,
            "transaction_index": 0,
            "from_address": tx["result"].get("from"),
            "to_address": tx["result"].get("to"),
            "value": str(tx["result"].get("value", "0x0")),
            "input": tx["result"].get("input"),
            "output": tx["result"].get("output"),
            "trace_type": tx["result"].get("type", "call"),
            "call_type": tx["result"].get("callType"),
            "reward_type": None,
            "gas": tx["result"].get("gas"),
            "gas_used": tx["result"].get("gasUsed"),
            "subtraces": len(tx["result"].get("calls", [])),
            "trace_address": "",  # Root trace
            "error": tx["result"].get("error"),
            "status": 1 if not tx["result"].get("error") else 0,
            "trace_id": tx_hash,
            "block_number": block_number,
            "block_hash": block_hash,
            "block_timestamp": datetime.fromtimestamp(block_timestamp),
        }
        all_traces.append(main_trace)

        # Process internal traces if they exist
        if "calls" in tx["result"] and tx["result"]["calls"]:
            parse_tasks.append(
                process_internal_traces(
                    calls=tx["result"]["calls"],
                    parent_tx_hash=tx_hash,
                    block_number=block_number,
                    block_hash=block_hash,
                    block_timestamp=block_timestamp,
                    trace_address="",
                )
            )

    # Wait for all internal traces to be processed
    if parse_tasks:
        internal_traces_lists = await asyncio.gather(*parse_tasks)
        for traces in internal_traces_lists:
            all_traces.extend(traces)

    return all_traces


async def process_internal_traces(
    calls: List[Dict],
    parent_tx_hash: str,
    block_number: int,
    block_hash: str,
    block_timestamp: int,
    trace_address: str,
) -> List[Dict]:
    """
    Process internal traces concurrently.

    Args:
        calls: List of internal calls to process
        parent_tx_hash: Hash of the parent transaction
        block_number: Block number
        block_hash: Block hash
        block_timestamp: Block timestamp
        trace_address: Current trace address path

    Returns:
        List of processed internal traces
    """
    traces = []
    nested_tasks = []

    for idx, call in enumerate(calls):
        if not call:  # Skip empty calls
            continue

        current_trace_address = f"{trace_address}-{idx}" if trace_address else str(idx)

        # Create trace entry
        trace = {
            "transaction_hash": parent_tx_hash,
            "transaction_index": 0,
            "from_address": call.get("from"),
            "to_address": call.get("to"),
            "value": str(call.get("value", "0x0")),
            "input": call.get("input"),
            "output": call.get("output"),
            "trace_type": call.get("type", "call"),
            "call_type": call.get("callType"),
            "reward_type": None,
            "gas": call.get("gas"),
            "gas_used": call.get("gasUsed"),
            "subtraces": len(call.get("calls", [])),
            "trace_address": current_trace_address,
            "error": call.get("error"),
            "status": 1 if not call.get("error") else 0,
            "trace_id": f"{parent_tx_hash}-{current_trace_address}",
            "block_number": block_number,
            "block_hash": block_hash,
            "block_timestamp": datetime.fromtimestamp(block_timestamp),
        }
        traces.append(trace)

        # Process nested calls concurrently
        if call.get("calls"):
            nested_tasks.append(
                process_internal_traces(
                    calls=call["calls"],
                    parent_tx_hash=parent_tx_hash,
                    block_number=block_number,
                    block_hash=block_hash,
                    block_timestamp=block_timestamp,
                    trace_address=current_trace_address,
                )
            )

    # Wait for all nested traces to be processed
    if nested_tasks:
        nested_traces_lists = await asyncio.gather(*nested_tasks)
        for nested_traces in nested_traces_lists:
            traces.extend(nested_traces)

    return traces
