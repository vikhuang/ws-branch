"""Broker Repository: Access to broker metadata.

Provides read access to:
- broker_names.json (broker code to name mapping)
- 證券商基本資料.xls (official broker master data)
"""

import json

from pnl_analytics.infrastructure.repositories.base import Repository, RepositoryError
from pnl_analytics.infrastructure.config import DataPaths, DEFAULT_PATHS


class BrokerRepository(Repository[dict[str, str]]):
    """Repository for broker name mappings.

    Merges data from multiple sources:
    1. broker_names.json (base data)
    2. 證券商基本資料.xls (official data, takes precedence)

    Example:
        >>> repo = BrokerRepository()
        >>> names = repo.get_all()  # {broker_code: name}
        >>> name = repo.get_name("1440")  # "美林"
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: dict[str, str] | None = None

    def get_all(self) -> dict[str, str]:
        """Load all broker names.

        Returns:
            Dict mapping broker code to name

        Raises:
            RepositoryError: If no broker names found
        """
        if self._cache is not None:
            return self._cache

        broker_names: dict[str, str] = {}

        # Load from JSON first (base data)
        json_path = self._paths.broker_names
        if json_path.exists():
            try:
                with open(json_path, encoding="utf-8") as f:
                    broker_names = json.load(f)
            except Exception:
                pass

        # Override/add from XLS (official data)
        xls_path = self._paths.broker_master
        if xls_path.exists():
            try:
                import xlrd
                wb = xlrd.open_workbook(str(xls_path))
                sheet = wb.sheet_by_index(0)
                for row_idx in range(1, sheet.nrows):
                    code = str(sheet.cell_value(row_idx, 0)).strip()
                    name = str(sheet.cell_value(row_idx, 1)).strip()
                    if code and name:
                        broker_names[code] = name
            except ImportError:
                pass
            except Exception:
                pass

        if not broker_names:
            raise RepositoryError(
                "No broker names found",
                f"{json_path} or {xls_path}"
            )

        self._cache = broker_names
        return self._cache

    def get_name(self, broker: str) -> str:
        """Get name for a specific broker.

        Args:
            broker: Broker code (e.g., "1440")

        Returns:
            Broker name, or empty string if not found
        """
        return self.get_all().get(broker, "")

    def get_names(self, brokers: list[str]) -> dict[str, str]:
        """Get names for multiple brokers.

        Args:
            brokers: List of broker codes

        Returns:
            Dict mapping broker code to name
        """
        all_names = self.get_all()
        return {b: all_names.get(b, "") for b in brokers}

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None
