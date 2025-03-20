from web3 import Web3
from web3.middleware import geth_poa_middleware
from typing import Optional
from poly_vision.utils.config import BlockchainConfig

class BlockchainService:
    """Service for interacting with blockchain."""

    def __init__(self, config: BlockchainConfig):
        self.config = config
        self.w3 = Web3(Web3.HTTPProvider(config.rpc_url))

        # Add PoA middleware for Polygon
        self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)

    async def get_block_with_transactions(self, block_number: int):
        """Get block with full transaction objects."""
        try:
            block = self.w3.eth.get_block(block_number, full_transactions=True)
            return block
        except Exception as e:
            raise Exception(f"Failed to get block {block_number}: {str(e)}")

    async def get_latest_block_number(self) -> int:
        """Get the latest block number from the blockchain."""
        try:
            return self.w3.eth.block_number
        except Exception as e:
            raise Exception(f"Failed to get latest block number: {str(e)}")

    async def setup_block_subscription(self, callback):
        """Setup websocket subscription for new blocks."""
        ws_provider = Web3.WebsocketProvider(
            self.config.ws_url, websocket_timeout=self.config.timeout
        )
        ws_w3 = Web3(ws_provider)

        subscription = ws_w3.eth.subscribe("newHeads")
        subscription.watch(callback)
        return subscription
