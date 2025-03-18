from prisma.client import Prisma
from typing import Optional
from poly_vision.utils.config import DatabaseConfig
from poly_vision.utils.enums import (
    BlockData,
    TransactionData,
)


class DatabaseService:
    """Service for database operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.prisma = Prisma()

    async def connect(self):
        """Connect to database."""
        await self.prisma.connect()

    async def disconnect(self):
        """Disconnect from database."""
        await self.prisma.disconnect()

    async def save_block(self, block_data: BlockData):
        """Save block data to database."""
        try:
            await self.prisma.blocks.create(data=block_data.dict())
        except Exception as e:
            raise Exception(f"Failed to save block {block_data.number}: {str(e)}")

    async def save_transaction(self, tx_data: TransactionData):
        """Save transaction data to database."""
        try:
            await self.prisma.transactions.create(data=tx_data.dict())
        except Exception as e:
            raise Exception(f"Failed to save transaction {tx_data.hash}: {str(e)}")

    async def get_latest_block_number(self) -> Optional[int]:
        """Get the latest block number from the database."""
        try:
            latest_block = await self.prisma.blocks.find_first(order={"number": "desc"})
            return latest_block.number if latest_block else None
        except Exception as e:
            raise Exception(f"Failed to get latest block: {str(e)}")
