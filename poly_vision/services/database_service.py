from prisma.client import Prisma
from typing import Optional, List, Dict, Any
from datetime import datetime
from poly_vision.utils.config import DatabaseConfig
from poly_vision.utils.enums import (
    BlockData,
    Transaction,
)
import asyncio


class DatabaseService:
    """Service for database operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.prisma = Prisma()
        self._is_connected = False  # Track connection state

    async def connect(self):
        """Connect to database if not already connected."""
        if not self._is_connected:
            await self.prisma.connect()
            self._is_connected = True

    async def disconnect(self):
        """Disconnect from database if connected."""
        if self._is_connected:
            await self.prisma.disconnect()
            self._is_connected = False

    async def save_block(self, block_data: BlockData):
        """Save block data to database."""
        try:
            # Convert timestamp from int to datetime
            timestamp = datetime.fromtimestamp(block_data.timestamp)

            # Create block data with all fields
            block = {
                "timestamp": timestamp,
                "number": block_data.block_number,
                "hash": block_data.hash,
                "parent_hash": block_data.parent_hash,
                "nonce": block_data.nonce,
                "sha3_uncles": block_data.sha3_uncles,
                "logs_bloom": block_data.logs_bloom,
                "transactions_root": block_data.transactions_root,
                "state_root": block_data.state_root,
                "receipts_root": block_data.receipts_root,
                "miner": block_data.miner,
                "difficulty": 0,
                "total_difficulty": 0,
                "size": self.validate_int4(block_data.size),
                "extra_data": block_data.extra_data,
                "gas_limit": -2,
                "gas_used": -2,
                "transaction_count": block_data.transaction_count,
                "base_fee_per_gas": -2,
            }

            await self.prisma.blocks.create(data=block)
        except Exception as e:
            raise Exception(f"Failed to save block {block_data.block_number}: {str(e)}")

    async def save_transaction(self, tx: Transaction):
        """Save transaction data to database."""
        try:
            # Convert transaction data to match schema
            MAX_INT_32 = 2147483647

            tx_data = {
                "hash": tx.hash,
                "nonce": tx.nonce,
                "transaction_index": 0,  # Default value since it's required
                "from_address": tx.from_,
                "to_address": tx.to,  # Optional in schema
                "value": (
                    str(tx.value) if tx.value is not None else None
                ),  # Optional Decimal
                "gas": tx.gas if tx.gas is not None else None,  # Optional Int
                "gas_price": (
                    tx.gasPrice
                    if tx.gasPrice is not None and tx.gasPrice <= MAX_INT_32
                    else (-2 if tx.gasPrice is not None else None)
                ),
                "input": tx.input if tx.input else None,  # Optional String
                "receipt_cumulative_gas_used": None,  # Optional Int
                "receipt_gas_used": (
                    tx.gas_used
                    if tx.gas_used is not None and tx.gas_used <= MAX_INT_32
                    else (-2 if tx.gas_used is not None else None)
                ),
                "receipt_contract_address": None,  # Optional String
                "receipt_root": None,  # Optional String
                "receipt_status": (
                    tx.status if tx.status is not None else None
                ),  # Optional Int
                "block_timestamp": datetime.fromtimestamp(
                    tx.blockNumber
                ),  # Required DateTime
                "block_number": tx.blockNumber,  # Required Int
                "block_hash": tx.blockHash,  # Required String
                "max_fee_per_gas": (
                    tx.maxFeePerGas
                    if tx.maxFeePerGas is not None and tx.maxFeePerGas <= MAX_INT_32
                    else (-2 if tx.maxFeePerGas is not None else None)
                ),
                "max_priority_fee_per_gas": (
                    tx.maxPriorityFeePerGas
                    if tx.maxPriorityFeePerGas is not None
                    and tx.maxPriorityFeePerGas <= MAX_INT_32
                    else (-2 if tx.maxPriorityFeePerGas is not None else None)
                ),
                "transaction_type": (
                    tx.type if tx.type is not None else None
                ),  # Optional Int
                "receipt_effective_gas_price": None,  # Optional Int
            }

            await self.prisma.transactions.create(data=tx_data)

            # Save traces if they exist
            if tx.traces:
                await self.save_traces(tx.traces, tx.hash, tx.blockNumber, tx.blockHash)

        except Exception as e:
            raise Exception(f"Failed to save transaction {tx.hash}: {str(e)}")

    def validate_int4(self, value):
        INT4_MAX = 2147483647
        if value is None:
            return None
        return value if value <= INT4_MAX else -2

    async def save_traces(
        self, traces: Dict[str, Any], tx_hash: str, block_number: int, block_hash: str
    ):
        """Save transaction traces to database."""
        try:

            async def process_trace(
                trace: Dict[str, Any], trace_address: str = ""
            ) -> Dict:
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
                    "trace_id": (
                        f"{tx_hash}-{trace_address}" if trace_address else tx_hash
                    ),
                    "block_number": block_number,
                    "block_hash": block_hash,
                    "block_timestamp": datetime.fromtimestamp(block_number),
                }

            # Process main trace
            trace_data = await process_trace(traces)
            await self.prisma.traces.create(data=trace_data)

            # Process nested calls concurrently
            async def save_nested_traces(parent_trace: Dict, parent_address: str):
                tasks = []
                for i, call in enumerate(parent_trace.get("calls", [])):
                    trace_address = (
                        f"{parent_address}-{i}" if parent_address else str(i)
                    )
                    nested_trace = await process_trace(call, trace_address)
                    tasks.append(self.prisma.traces.create(data=nested_trace))
                    tasks.append(save_nested_traces(call, trace_address))

                if tasks:
                    await asyncio.gather(*tasks)

            await save_nested_traces(traces, "")

        except Exception as e:
            raise Exception(
                f"Failed to save traces for transaction {tx_hash}: {str(e)}"
            )

    async def save_transactions_batch(self, transactions: List[Transaction]):
        """Save multiple transactions in a batch."""
        for tx in transactions:
            await self.save_transaction(tx)

    async def get_latest_block_number(self) -> Optional[int]:
        """Get the latest block number from the database."""
        try:
            latest_block = await self.prisma.blocks.find_first(order={"number": "desc"})
            return latest_block.number if latest_block else None
        except Exception as e:
            raise Exception(f"Failed to get latest block: {str(e)}")

    async def process_traces_for_saving(
        self,
        trace: Dict[str, Any],
        tx_hash: str,
        block_number: int,
        block_hash: str,
        block_timestamp: int,
        parent_address: str = "",
        depth: int = 0,
        max_depth: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Process traces recursively and prepare them for saving.

        Args:
            trace: The trace data to process
            tx_hash: Transaction hash
            block_number: Block number
            block_hash: Block hash
            block_timestamp: Block timestamp
            parent_address: Parent trace address for nested calls
            depth: Current recursion depth
            max_depth: Maximum recursion depth

        Returns:
            List of processed traces ready for database insertion
        """
        if depth > max_depth:
            print(f"Max depth {max_depth} reached for tx {tx_hash}")
            return []

        traces_to_save = []

        try:
            # Process single trace
            trace_data = {
                "transaction_hash": tx_hash,
                "transaction_index": 0,
                "from_address": trace.get("from"),
                "to_address": trace.get("to"),
                "value": str(
                    trace.get("value", "0")
                ),  # Ensure string for large numbers
                "input": trace.get("input"),
                "output": trace.get("output"),
                "trace_type": trace.get("type", "call"),
                "call_type": trace.get("callType"),
                "reward_type": None,
                "gas": self._safe_hex_to_int(trace.get("gas")),
                "gas_used": self._safe_hex_to_int(trace.get("gasUsed")),
                "subtraces": len(trace.get("calls", [])),
                "trace_address": parent_address,
                "error": trace.get("error"),
                "status": 1 if not trace.get("error") else 0,
                "trace_id": (
                    f"{tx_hash}-{parent_address}" if parent_address else tx_hash
                ),
                "block_number": block_number,
                "block_hash": block_hash,
                "block_timestamp": self._safe_timestamp_to_datetime(block_timestamp),
            }

            traces_to_save.append(trace_data)

            # Process nested calls
            for i, call in enumerate(trace.get("calls", [])):
                if not isinstance(call, dict):
                    print(f"Invalid call data in tx {tx_hash}")
                    continue

                trace_address = f"{parent_address}-{i}" if parent_address else str(i)
                nested_traces = await self.process_traces_for_saving(
                    call,
                    tx_hash,
                    block_number,
                    block_hash,
                    block_timestamp,
                    trace_address,
                    depth + 1,
                    max_depth,
                )
                traces_to_save.extend(nested_traces)

            return traces_to_save

        except Exception as e:
            print(f"Error processing trace for tx {tx_hash}: {str(e)}")
            return traces_to_save

    def _safe_hex_to_int(self, hex_value: Any) -> int:
        """Safely convert hex value to int."""
        try:
            if isinstance(hex_value, str):
                return int(hex_value, 16)
            elif isinstance(hex_value, int):
                return hex_value
            return 0
        except ValueError:
            return 0

    def _safe_timestamp_to_datetime(self, timestamp: Any) -> datetime:
        """Safely convert timestamp to datetime."""
        try:
            return datetime.fromtimestamp(int(timestamp))
        except (ValueError, TypeError):
            return datetime.utcnow()

    async def save_traces_batch(self, traces: List[Dict[str, Any]]):
        """Save multiple traces in a batch."""
        try:
            # Use Prisma's create_many if available, or fall back to individual creates
            for trace in traces:
                await self.prisma.traces.create(data=trace)
        except Exception as e:
            raise Exception(f"Failed to save traces batch: {str(e)}")
