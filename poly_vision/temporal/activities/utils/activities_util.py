import asyncio
from datetime import datetime
from typing import Dict, List


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
