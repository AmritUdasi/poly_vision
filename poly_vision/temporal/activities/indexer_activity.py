from temporalio import activity
from typing import Dict, Any, Optional
from poly_vision.services.blockchain_service import BlockchainService
from poly_vision.services.database_service import DatabaseService
from poly_vision.utils.config import load_config
from poly_vision.utils.enums import (
    IndexingResult,
    IndexingStatus,
    IndexingErrorType,
    TransactionData,
)
import asyncio

# Load config once at module level
config = load_config()
blockchain_service = BlockchainService(config.blockchain)
db_service = DatabaseService(config.database)


@activity.defn
async def index_block(block_number: int) -> Any:
    """Index a specific block from Polygon network."""
    try:
        # Get block data
        block_data = await blockchain_service.get_block_with_transactions(block_number)

        # print(f"""{block_data=}""")
        # return block_data
        # Save block data
        # await db_service.save_block(block_data)

        # Process transactions
        return block_data[0]
    
        print(f'''tttttttttttttttttttttttttttttttttttttttttttt{block_data.transactions=}''')
        for tx in block_data.transactions:
            tx_receipt = blockchain_service.w3.eth.get_transaction_receipt(tx.hash)
            print(f'''recccccccccccccccccccccccccccceeeeeeeeeeeeeeeeeeeeeeeeipttttttttp{tx_receipt=}''')
            tx_trace = await blockchain_service.w3.provider.make_request(
                "debug_traceTransaction", [tx.hash.hex(), {"tracer": "callTracer"}]
            )
            return tx_trace
            print(f'''traceeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee{tx_trace=}''')
            break
            tx_data = TransactionData(
                hash=tx.hash.hex(),
                nonce=tx.nonce,
                block_hash=tx.blockHash.hex(),
                block_number=tx.blockNumber,
                transaction_index=tx.transactionIndex,
                from_address=tx["from"],
                to_address=tx.to if tx.to else None,
                value=tx.value,
                gas=tx.gas,
                gas_price=tx.gasPrice,
                input=tx.input,
                block_timestamp=block_data.timestamp,
                transaction_type=tx.type if hasattr(tx, "type") else None,
                max_fee_per_gas=(
                    tx.maxFeePerGas if hasattr(tx, "maxFeePerGas") else None
                ),
                max_priority_fee_per_gas=(
                    tx.maxPriorityFeePerGas
                    if hasattr(tx, "maxPriorityFeePerGas")
                    else None
                ),
                receipt_cumulative_gas_used=tx_receipt.cumulativeGasUsed,
                receipt_gas_used=tx_receipt.gasUsed,
                receipt_contract_address=tx_receipt.contractAddress,
                receipt_status=tx_receipt.status,
                receipt_effective_gas_price=(
                    tx_receipt.effectiveGasPrice
                    if hasattr(tx_receipt, "effectiveGasPrice")
                    else None
                ),
            )
            # await db_service.save_transaction(tx_data)
        return "heloooo"
        return IndexingResult(
            status=IndexingStatus.SUCCESS,
            block_number=block_number,
            transactions_indexed=len(block_data.transactions),
        )

    except Exception as e:
        return None


@activity.defn
async def index_block_range(
    start_block: int, end_block: int, batch_size: Optional[int] = None
) -> Dict[str, Any]:
    """Index a range of blocks from Polygon network."""
    batch_size = batch_size or config.batch_size
    results = []
    temp = await index_block(start_block)
    print(f'''{temp=}''')
    return temp
    for block_number in range(start_block, end_block + 1, batch_size):
        batch_end = min(block_number + batch_size, end_block + 1)
        batch_tasks = [
            index_block(block_num) for block_num in range(block_number, batch_end)
        ]
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        results.extend(batch_results)
    return results
    return {
        "status": IndexingStatus.COMPLETED.value,
        "blocks_processed": len(results),
        "start_block": start_block,
        "end_block": end_block,
        "results": results,
    }


@activity.defn
async def get_latest_blocks() -> Dict[str, Any]:
    """Get latest block numbers from both DB and blockchain."""
    try:
        # Connect to database first
        await db_service.prisma.connect()

        # Get latest block from DB
        latest_db_block = await db_service.get_latest_block_number()
        print(f"""{latest_db_block=}""")

        # Get latest block from chain
        latest_chain_block = await blockchain_service.get_latest_block_number()
        print(f"""{latest_chain_block=}""")

        # Disconnect after we're done
        await db_service.prisma.disconnect()

        return {"db_block": latest_db_block, "chain_block": latest_chain_block}
    except Exception as e:
        # Make sure to disconnect even if there's an error
        try:
            await db_service.prisma.disconnect()
        except:
            pass
        raise e
