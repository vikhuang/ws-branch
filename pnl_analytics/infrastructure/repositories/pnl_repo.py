"""PNL Repository: Access to PNL tensor data.

Provides read access to:
- realized_pnl.npy (3D tensor of realized PNL)
- unrealized_pnl.npy (3D tensor of unrealized PNL)
"""

from pathlib import Path

import numpy as np

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class PnlRepository(Repository[tuple[np.ndarray, np.ndarray]]):
    """Repository for PNL tensor data.

    Provides access to realized and unrealized PNL tensors.

    Tensor shape: (n_symbols, n_dates, n_brokers)
    - Axis 0: Symbols (usually just 1 for single-stock analysis)
    - Axis 1: Dates
    - Axis 2: Brokers

    Example:
        >>> repo = PnlRepository()
        >>> realized, unrealized = repo.get_all()
        >>> broker_realized = repo.get_broker_realized(108)  # by index
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._realized_cache: np.ndarray | None = None
        self._unrealized_cache: np.ndarray | None = None

    def get_all(self) -> tuple[np.ndarray, np.ndarray]:
        """Load both PNL tensors.

        Returns:
            Tuple of (realized_pnl, unrealized_pnl) arrays

        Raises:
            RepositoryError: If files cannot be read
        """
        return self.get_realized(), self.get_unrealized()

    def get_realized(self) -> np.ndarray:
        """Load realized PNL tensor.

        Returns:
            3D numpy array of daily realized PNL

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._realized_cache is not None:
            return self._realized_cache

        path = self._paths.realized_pnl
        if not path.exists():
            raise RepositoryError(f"Realized PNL file not found", str(path))

        try:
            self._realized_cache = np.load(path)
            return self._realized_cache
        except Exception as e:
            raise RepositoryError(f"Failed to read realized PNL: {e}", str(path))

    def get_unrealized(self) -> np.ndarray:
        """Load unrealized PNL tensor.

        Returns:
            3D numpy array of daily unrealized PNL

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._unrealized_cache is not None:
            return self._unrealized_cache

        path = self._paths.unrealized_pnl
        if not path.exists():
            raise RepositoryError(f"Unrealized PNL file not found", str(path))

        try:
            self._unrealized_cache = np.load(path)
            return self._unrealized_cache
        except Exception as e:
            raise RepositoryError(f"Failed to read unrealized PNL: {e}", str(path))

    def get_broker_realized(self, broker_idx: int, symbol_idx: int = 0) -> np.ndarray:
        """Get realized PNL time series for a broker.

        Args:
            broker_idx: Broker index in tensor
            symbol_idx: Symbol index (default 0)

        Returns:
            1D array of daily realized PNL
        """
        realized = self.get_realized()
        return realized[symbol_idx, :, broker_idx]

    def get_broker_unrealized(self, broker_idx: int, symbol_idx: int = 0) -> np.ndarray:
        """Get unrealized PNL time series for a broker.

        Args:
            broker_idx: Broker index in tensor
            symbol_idx: Symbol index (default 0)

        Returns:
            1D array of daily unrealized PNL
        """
        unrealized = self.get_unrealized()
        return unrealized[symbol_idx, :, broker_idx]

    def get_broker_total_realized(self, broker_idx: int, symbol_idx: int = 0) -> float:
        """Get total realized PNL for a broker.

        Args:
            broker_idx: Broker index in tensor
            symbol_idx: Symbol index (default 0)

        Returns:
            Sum of all realized PNL
        """
        return float(self.get_broker_realized(broker_idx, symbol_idx).sum())

    def get_broker_final_unrealized(self, broker_idx: int, symbol_idx: int = 0) -> float:
        """Get final unrealized PNL for a broker.

        Args:
            broker_idx: Broker index in tensor
            symbol_idx: Symbol index (default 0)

        Returns:
            Unrealized PNL at the last date
        """
        unrealized = self.get_broker_unrealized(broker_idx, symbol_idx)
        return float(unrealized[-1])

    def get_total_realized(self) -> float:
        """Get total realized PNL across all brokers."""
        return float(self.get_realized().sum())

    def get_total_final_unrealized(self) -> float:
        """Get total final unrealized PNL across all brokers."""
        unrealized = self.get_unrealized()
        return float(unrealized[0, -1, :].sum())

    def get_shape(self) -> tuple[int, int, int]:
        """Get tensor shape (n_symbols, n_dates, n_brokers)."""
        return self.get_realized().shape

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._realized_cache = None
        self._unrealized_cache = None
