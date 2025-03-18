from web3 import Web3
from web3.middleware import geth_poa_middleware
from typing import Optional
from poly_vision.utils.config import BlockchainConfig
from poly_vision.utils.enums import (
    BlockData,
)
from datetime import datetime


class BlockchainService:
    """Service for interacting with blockchain."""

    def __init__(self, config: BlockchainConfig):
        self.config = config
        self.w3 = Web3(
            Web3.HTTPProvider(
                str(config.rpc_url), request_kwargs={"timeout": config.timeout}
            )
        )

        # Add PoA middleware for Polygon
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    async def get_block_with_transactions(
        self, block_number: int
    ) -> Optional[BlockData]:
        """Fetch block data with transactions."""
        try:
            block = self.w3.eth.get_block(block_number, full_transactions=True)
            return BlockData(
                number=block_number,
                hash=block.hash.hex(),
                parent_hash=block.parentHash.hex(),
                nonce=block.nonce.hex(),
                timestamp=datetime.fromtimestamp(block.timestamp),
                transactions_root=block.transactionsRoot.hex(),
                state_root=block.stateRoot.hex(),
                receipts_root=block.receiptsRoot.hex(),
                miner=block.miner,
                difficulty=block.difficulty,
                total_difficulty=block.totalDifficulty,
                size=block.size,
                gas_limit=block.gasLimit,
                gas_used=block.gasUsed,
                base_fee_per_gas=(
                    block.baseFeePerGas if hasattr(block, "baseFeePerGas") else None
                ),
                transaction_count=len(block.transactions),
            )
        except Exception as e:
            raise Exception(f"Failed to fetch block {block_number}: {str(e)}")

    async def get_latest_block_number(self) -> int:
        """Get the latest block number from the chain."""
        return self.w3.eth.block_number

    async def setup_block_subscription(self, callback):
        """Setup websocket subscription for new blocks."""
        ws_provider = Web3.WebsocketProvider(
            self.config.ws_url, websocket_timeout=self.config.timeout
        )
        ws_w3 = Web3(ws_provider)

        subscription = ws_w3.eth.subscribe("newHeads")
        subscription.watch(callback)
        return subscription
