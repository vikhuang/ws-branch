"""Base Repository: Abstract interface for data access.

Repository Pattern provides:
- Abstraction over data sources (files, databases, APIs)
- Caching for performance
- Consistent error handling
"""

from abc import ABC, abstractmethod
from typing import TypeVar, Generic

T = TypeVar("T")


class Repository(ABC, Generic[T]):
    """Abstract base class for repositories."""

    @abstractmethod
    def get_all(self) -> T:
        """Retrieve all data."""
        pass

    @abstractmethod
    def clear_cache(self) -> None:
        """Clear any cached data."""
        pass


class RepositoryError(Exception):
    """Exception raised when repository operations fail."""

    def __init__(self, message: str, path: str | None = None):
        self.path = path
        super().__init__(f"{message}" + (f" (path: {path})" if path else ""))
