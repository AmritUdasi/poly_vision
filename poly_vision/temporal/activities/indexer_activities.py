from temporalio import activity
from typing import Dict, Any, List
from poly_vision.services.blockchain_service import BlockchainService
from poly_vision.services.database_service import DatabaseService
from poly_vision.temporal.activities.utils.activities_util import parse_traces
from poly_vision.utils.config import load_config
from poly_vision.utils.enums import (
    BlockData,
    Transaction,
)
import asyncio

# Load config once at module level
# TODO: create initialization class
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
        trace_result = await blockchain_service.trace_block(block.number)

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

            except Exception as e:
                print(f"Failed to save transactions for block {block_number}: {str(e)}")

        if traces_batch:
            try:
                await db_service.save_traces_batch(traces_batch)
            except Exception as e:
                print(f"Failed to save traces for block {block_number}: {str(e)}")

        await db_service.disconnect()
        return block_number

    except Exception as e:
        await db_service.disconnect()
        raise Exception(f"Failed to index block {block_number}: {str(e)}")


@activity.defn
async def index_block_range(start_block: int, end_block: int) -> List[Dict]:
    """Index a range of blocks from the blockchain."""
    failed_blocks = []
    max_retries = 3  # Number of retries for each block

    # Create a list of block numbers to process
    blocks = list(range(start_block, end_block + 1))

    # Process blocks one at a time
    for block_number in blocks:
        retries = 0
        success = False

        while retries < max_retries and not success:
            try:
                # Process block
                await index_block(block_number)
                activity.logger.info(f"Successfully processed block {block_number}")
                success = True
            except Exception as e:
                retries += 1
                activity.logger.error(
                    f"Attempt {retries}/{max_retries} - Failed to process block {block_number}: {str(e)}"
                )
                if retries == max_retries:
                    failed_blocks.append({"block": block_number, "error": str(e)})
                    activity.logger.error(
                        f"Block {block_number} failed after {max_retries} attempts. Moving to next block."
                    )
                else:
                    # Wait before retrying (exponential backoff)
                    await asyncio.sleep(2 * retries)
                    continue

    return failed_blocks


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
