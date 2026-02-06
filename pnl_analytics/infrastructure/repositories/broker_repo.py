"""Broker Repository: Access to broker metadata.

Provides read access to:
- broker_names.json (broker code to name mapping)
- 證券商基本資料.xls (official broker master data)
- index_maps.json (dimension mappings)
"""

import json
from pathlib import Path

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
            RepositoryError: If primary file cannot be read
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
            except Exception as e:
                # JSON is optional, log warning but continue
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
                # xlrd not installed, skip XLS
                pass
            except Exception as e:
                # XLS read error, log warning but continue
                pass

        if not broker_names:
            raise RepositoryError(
                "No broker names could be loaded from any source",
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


class IndexMapRepository(Repository[dict]):
    """Repository for dimension index mappings.

    Provides access to mappings from string identifiers
    to integer indices for tensor operations.
    """

    def __init__(self, paths: DataPaths = DEFAULT_PATHS):
        self._paths = paths
        self._cache: dict | None = None

    def get_all(self) -> dict:
        """Load all index mappings.

        Returns:
            Dict with 'dates', 'symbols', 'brokers' mappings

        Raises:
            RepositoryError: If file cannot be read
        """
        if self._cache is not None:
            return self._cache

        path = self._paths.index_maps
        if not path.exists():
            raise RepositoryError(f"Index maps file not found", str(path))

        try:
            with open(path, encoding="utf-8") as f:
                self._cache = json.load(f)
            return self._cache
        except Exception as e:
            raise RepositoryError(f"Failed to read index maps: {e}", str(path))

    def get_broker_index(self, broker: str) -> int | None:
        """Get index for a broker code."""
        return self.get_all().get("brokers", {}).get(broker)

    def get_date_index(self, date: str) -> int | None:
        """Get index for a date."""
        return self.get_all().get("dates", {}).get(date)

    def get_brokers(self) -> list[str]:
        """Get list of all broker codes in index order."""
        brokers_map = self.get_all().get("brokers", {})
        # Sort by index to get original order
        return sorted(brokers_map.keys(), key=lambda b: brokers_map[b])

    def get_dates(self) -> list[str]:
        """Get list of all dates in index order."""
        dates_map = self.get_all().get("dates", {})
        return sorted(dates_map.keys())

    def clear_cache(self) -> None:
        """Clear cached data."""
        self._cache = None
