from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

class StorageManager(ABC):
    """Abstract interface for storage backends."""

    @abstractmethod
    def setup(self) -> None:
        """Initialize schema / tables if necessary."""
        pass

    @abstractmethod
    def write_block(self, block: Dict[str, Any]) -> None:
        """Persist a block’s normalized data."""
        pass

    @abstractmethod
    def read_block(self, block_number: int) -> Optional[Dict[str, Any]]:
        """Read stored block by number, if exists."""
        pass

    @abstractmethod
    def write_transaction(self, tx: Dict[str, Any]) -> None:
        """Persist normalized transaction."""
        pass

    @abstractmethod
    def write_log(self, log: Dict[str, Any]) -> None:
        """Persist normalized log / event."""
        pass

    @abstractmethod
    def query_blocks(self, start: int, end: int) -> List[Dict[str, Any]]:
        """Return blocks in a range."""
        pass
