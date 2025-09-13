"""
Author: Louis Goodnews
Date: 2025-09-05
"""

import threading

from collections.abc import KeysView, ItemsView, ValuesView
from datetime import datetime
from typing import Any, Final, Iterator, List, Optional, Union


__all__: Final[List[str]] = [
    "PebbleCache",
    "PebbleCacheEntry",
    "PebbleCacheEntryFactory",
    "PebbleCacheEntryBuilder",
]


class PebbleCacheEntry:
    """
    A class to represent a cache entry.
    """

    def __init__(
        self,
        data: dict[str, Any],
    ) -> None:
        """
        Initialize a new PebbleCacheEntry object.

        Args:
            data (dict[str, Any]): The data to store in the cache entry.

        Returns:
            None
        """

        # Initialize the dirty flag in an instance variable
        self._dirty: bool = False

        # Store the passed data dict in an instance variable
        self._data: dict[str, Any] = data

        # Initialize the last accessed timestamp in an instance variable
        self._last_accessed: Optional[datetime] = None

        # Initialize the lock in an instance variable
        self._lock: Final[threading.Lock] = threading.Lock()

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the cache entry contains the given key.

        Args:
            key (str): The key to check for.

        Returns:
            bool: True if the cache entry contains the given key, False otherwise.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return True if the cache entry contains the given key, False otherwise
            return key in self._data

    def __eq__(
        self,
        other: Union[dict[str, Any], "PebbleCacheEntry"],
    ) -> bool:
        """
        Check if the cache entry is equal to the other cache entry.

        Args:
            other (Union[dict[str, Any], PebbleCacheEntry): The other cache entry to compare to.

        Returns:
            bool: True if the cache entry is equal to the other cache entry, False otherwise.
        """

        # Check if the other object is a PebbleCacheEntry object
        if not isinstance(
            other,
            (dict, PebbleCacheEntry),
        ):
            # Return False if the other object is not a PebbleCacheEntry object
            return False

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Compare the data dicts
            return self._data == other.data if isinstance(other, PebbleCacheEntry) else other

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get the value of the given key.

        Args:
            key (str): The key to get the value of.

        Returns:
            Any: The value of the given key.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the value of the given key
            return self._data[key]

    def __iter__(self) -> Iterator[str]:
        """
        Return an iterator over the keys in the cache entry.

        Returns:
            Iterator[str]: An iterator over the keys in the cache entry.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return an iterator over the keys in the cache entry
            return iter(self._data)

    def __len__(self) -> int:
        """
        Return the number of items in the cache entry.

        Returns:
            int: The number of items in the cache entry.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the number of items in the cache entry
            return len(self._data)

    def __repr__(self) -> str:
        """
        Return the string representation of the object.

        Returns:
            str: The string representation of the object.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the string representation of the object
            return f"<{self.__class__.__name__}(data={self._data}, dirty={self._dirty}, last_accessed={str(self._last_accessed)})>"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set the value of the given key.

        Args:
            key (str): The key to set the value of.
            value (Any): The value to set.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Set the value of the given key
            self._data[key] = value

    def __str__(self) -> str:
        """
        Return the string representation of the object.

        Returns:
            str: The string representation of the object.
        """

        return self.__repr__()

    @property
    def data(self) -> dict[str, Any]:
        """
        Get the data stored in the cache entry.

        Returns:
            dict[str, Any]: The data stored in the cache entry.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the data stored in the cache entry
            return self._data

    @property
    def dirty(self) -> bool:
        """
        Get the dirty flag of the cache entry.

        Returns:
            bool: The dirty flag of the cache entry.
        """

        return self._dirty

    @dirty.setter
    def dirty(
        self,
        value: bool,
    ) -> None:
        """
        Set the dirty flag of the cache entry.

        Args:
            value (bool): The dirty flag of the cache entry.

        Returns:
            None
        """

        self._dirty = value

    @property
    def last_accessed(self) -> Optional[datetime]:
        """
        Get the last accessed timestamp of the cache entry.

        Returns:
            Optional[datetime]: The last accessed timestamp of the cache entry.
        """

        return self._last_accessed

    @last_accessed.setter
    def last_accessed(
        self,
        value: datetime,
    ) -> None:
        """
        Set the last accessed timestamp of the cache entry.

        Args:
            value (datetime): The last accessed timestamp of the cache entry.

        Returns:
            None
        """

        self._last_accessed = value

    def clear(self) -> None:
        """
        Clear all data from the cache entry.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Clear the data
            self._data.clear()

        # Mark the cache entry as dirty
        self.mark_as_dirty()

    def copy(self) -> "PebbleCacheEntry":
        """
        Create a shallow copy of the cache entry.

        Returns:
            A new PebbleCacheEntry with the same data
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return a new PebbleCacheEntry with the same data
            return PebbleCacheEntry(self._data.copy())

    def get(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        """
        Get the value for the given key, returning default if key is not found.

        Args:
            key: The key to look up
            default: Default value if key is not found

        Returns:
            The value for the key or default if not found
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the value for the given key, returning default if key is not found
            return self._data.get(key, default)

    def is_dirty(self) -> bool:
        """
        Check if the cache entry is dirty.

        Returns:
            bool: True if the cache entry is dirty, False otherwise.
        """

        return self._dirty

    def is_expired(
        self,
        time_to_live: int = 60,
    ) -> bool:
        """
        Check if the cache entry is expired.

        Args:
            time_to_live (int): The time to live in seconds. Defaults to 60.

        Returns:
            bool: True if the cache entry is expired, False otherwise.
        """

        return is_stale(
            interval=time_to_live,
            timestamp=self._last_accessed,
        )

    def mark_as_clean(self) -> None:
        """
        Mark the cache entry as clean.

        Returns:
            None
        """

        self._dirty = False

    def mark_as_dirty(self) -> None:
        """
        Mark the cache entry as dirty.

        Returns:
            None
        """

        self._dirty = True

    def pop(
        self,
        key: str,
        default: Any = None,
    ) -> Any:
        """
        Remove specified key and return the corresponding value.

        Args:
            key: Key to remove
            default: Default value if key is not found

        Returns:
            The value for the key or default if not found
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Pop the key from the data
            value: Any = self._data.pop(key, default)

        # Mark the cache entry as dirty
        self.mark_as_dirty()

        # Return the value
        return value

    def size(self) -> int:
        """
        Return the size of the cache entry.

        Returns:
            int: The size of the cache entry.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the size of the cache entry
            return len(self._data)

    def update(
        self,
        other: Union[dict, "PebbleCacheEntry"],
    ) -> None:
        """
        Update the cache entry with values from another dictionary or cache entry.

        Args:
            other: Dictionary or PebbleCacheEntry with values to update

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Get the data from the other object
            data: dict[str, Any] = other.data if isinstance(other, PebbleCacheEntry) else other

            # Update the data
            self._data.update(data)

        # Mark the cache entry as dirty
        self.mark_as_dirty()

    def update_last_accessed(self) -> None:
        """
        Update the last accessed timestamp.

        Returns:
            None
        """

        # Update the last accessed timestamp
        self._last_accessed = datetime.now()

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the cache entry to a dictionary.

        Returns:
            Dictionary representation of the cache entry
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the dictionary representation of the cache entry
            return {
                "data": self._data,
                "dirty": self._dirty,
                "last_accessed": self._last_accessed.isoformat() if self._last_accessed else None,
            }

    @classmethod
    def from_dict(
        cls,
        data: dict[str, Any],
    ) -> "PebbleCacheEntry":
        """
        Create a PebbleCacheEntry from a dictionary.

        Args:
            data: Dictionary with 'data', 'dirty', and 'last_accessed' keys

        Returns:
            A new PebbleCacheEntry
        """

        # Create a new PebbleCacheEntry from the dictionary
        entry: PebbleCacheEntry = cls(data["data"])

        # Set the dirty flag
        entry.dirty = data.get("dirty", False)

        # Check if the last accessed timestamp is in the dictionary
        if "last_accessed" in data and data["last_accessed"]:
            # Set the last accessed timestamp
            entry.last_accessed = datetime.fromisoformat(data["last_accessed"])

        # Return the new PebbleCacheEntry
        return entry


class PebbleCacheEntryFactory:
    """
    A class to represent a cache entry factory.
    """

    @classmethod
    def create(
        cls,
        data: dict[str, Any],
    ) -> "PebbleCacheEntry":
        """
        Create a PebbleCacheEntry from a dictionary.

        Args:
            data: Dictionary with 'data', 'dirty', and 'last_accessed' keys

        Returns:
            A new PebbleCacheEntry
        """

        # Create a new PebbleCacheEntry from the dictionary
        entry: PebbleCacheEntry = PebbleCacheEntry(data=data)

        # Update the last accessed timestamp
        entry.update_last_accessed()

        # Return the new PebbleCacheEntry
        return entry

    @classmethod
    def create_default(cls) -> "PebbleCacheEntry":
        """
        Create a default PebbleCacheEntry.

        Returns:
            A new PebbleCacheEntry
        """

        # Create a new PebbleCacheEntry
        entry: PebbleCacheEntry = PebbleCacheEntry(data={})

        # Update the last accessed timestamp
        entry.update_last_accessed()

        # Return the new PebbleCacheEntry
        return entry


class PebbleCacheEntryBuilder:
    """
    A class to represent a cache entry builder.
    """

    def __init__(self) -> None:
        """
        Initialize a new PebbleCacheEntryBuilder object.

        Returns:
            None
        """

        # Initialize the configuration dict as an instance variable
        self._configuration: Final[dict[str, Any]] = {}

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the configuration dict contains the given key.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the configuration dict contains the key, False otherwise.
        """

        return key in self._configuration

    def __eq__(
        self,
        other: "PebbleCacheEntryBuilder",
    ) -> bool:
        """
        Check if the configuration dict is equal to the other configuration dict.

        Args:
            other (PebbleCacheEntryBuilder): The other configuration dict.

        Returns:
            bool: True if the configuration dict is equal to the other configuration dict, False otherwise.
        """

        # Check if the other object is a PebbleCacheEntryBuilder
        if not isinstance(
            other,
            PebbleCacheEntryBuilder,
        ):
            # Return False
            return False

        # Return the comparison result
        return self._configuration == other._configuration

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get the value for the given key.

        Args:
            key (str): The key to look up.

        Returns:
            Any: The value for the key.
        """

        # Return the value for the given key
        return self._configuration[key]

    def __iter__(
        self,
    ) -> Iterator[str]:
        """
        Iterate over the keys in the configuration dict.

        Returns:
            Iterator[str]: An iterator over the keys in the configuration dict.
        """

        # Return an iterator over the keys in the configuration dict
        return iter(self._configuration)

    def __len__(
        self,
    ) -> int:
        """
        Get the length of the configuration dict.

        Returns:
            int: The length of the configuration dict.
        """

        # Return the length of the configuration dict
        return len(self._configuration)

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set the value for the given key.

        Args:
            key (str): The key to look up.
            value (Any): The value to set.

        Returns:
            None
        """

        # Set the value for the given key
        self._configuration[key] = value

    @property
    def configuration(self) -> dict[str, Any]:
        """
        Get the configuration dict.

        Returns:
            dict[str, Any]: The configuration dict.
        """

        # Return the configuration dict
        return self._configuration

    def build(self) -> PebbleCacheEntry:
        """
        Build the cache entry.

        Returns:
            PebbleCacheEntry: The cache entry.
        """

        # Return the cache entry
        return PebbleCacheEntryFactory.create(**self._configuration)

    def items(self) -> ItemsView[str, Any]:
        """
        Get the items in the configuration dict.

        Returns:
            ItemsView[str, Any]: The items in the configuration dict.
        """

        # Return the items in the configuration dict
        return self._configuration.items()

    def keys(self) -> KeysView[str]:
        """
        Get the keys in the configuration dict.

        Returns:
            KeysView[str]: The keys in the configuration dict.
        """

        # Return the keys in the configuration dict
        return self._configuration.keys()

    def values(self) -> ValuesView[Any]:
        """
        Get the values in the configuration dict.

        Returns:
            ValuesView[Any]: The values in the configuration dict.
        """

        # Return the values in the configuration dict
        return self._configuration.values()

    def with_data(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the data for the cache entry.

        Args:
            value (dict[str, Any]): The data to set.

        Returns:
            Self: The cache entry.
        """

        # Set the data for the cache entry
        self._configuration["data"] = value

        # Return the cache entry builder to the caller
        return self


class PebbleCache:
    """
    A class to represent a cache.
    """

    def __init__(
        self,
        cleanup_interval: int = 60,
        max_size: Optional[int] = None,
        time_to_live: int = 3600,
    ) -> None:
        """
        Initialize a new PebbleCache object.

        Args:
            cleanup_interval (int): The cleanup interval.
            max_size (Optional[int]): The max size.
            time_to_live (int): The time to live.

        Returns:
            None
        """

        # Initialize the cache
        self._cache: Final[dict[str, PebbleCacheEntry]] = {}

        # Store the cleanup interval in an instance variable
        self._cleanup_interval: Final[int] = cleanup_interval

        # Store the max size in an instance variable
        self._max_size: Final[Optional[int]] = max_size

        # Store the last cleaned at timestamp in an instance variable
        self._last_cleaned_at: Optional[datetime] = None

        # Store the lock in an instance variable
        self._lock: Final[threading.Lock] = threading.Lock()

        # Store the time to live in an instance variable
        self._time_to_live: Final[int] = time_to_live

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the cache contains the given key.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the cache contains the key, False otherwise.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return True if the cache contains the key, False otherwise
            return key in self._cache

    def __eq__(
        self,
        other: "PebbleCache",
    ) -> bool:
        """
        Check if the cache is equal to the other cache.

        Args:
            other (PebbleCache): The other cache to compare to.

        Returns:
            bool: True if the cache is equal to the other cache, False otherwise.
        """

        # Check if the other object is a PebbleCache object
        if not isinstance(
            other,
            PebbleCache,
        ):
            # Return False if the other object is not a PebbleCache object
            return False

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return True if the cache is equal to the other cache
            return self._cache == other._cache

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get the entry with the given key.

        Args:
            key (str): The key of the entry to get.

        Returns:
            Any: The entry with the given key.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Get the entry
            entry: PebbleCacheEntry = self._cache[key]

            # Update the last accessed timestamp
            entry.update_last_accessed()

            # Return the data
            return entry.data

    def __len__(self) -> int:
        """
        Return the number of items in the cache.

        Returns:
            int: The number of items in the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the number of items in the cache
            return len(self._cache)

    def __repr__(self) -> str:
        """
        Return the string representation of the cache.

        Returns:
            str: The string representation of the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the string representation of the cache
            return f"<{self.__class__.__name__}(cleanup_interval={self._cleanup_interval}, max_size={self._max_size}, time_to_live={self._time_to_live})"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set the entry with the given key.

        Args:
            key (str): The key of the entry to set.
            value (Any): The value of the entry to set.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Maybe cleanup
            self._maybe_cleanup()

            # Maybe evict
            self._maybe_evict()

            # Get the entry
            entry: Optional[PebbleCacheEntry] = self._cache.get(key, None)

            # Check if the entry is not None
            if not entry:
                # Create a new entry
                entry = (
                    PebbleCacheEntry(value)
                    if not isinstance(
                        value,
                        PebbleCacheEntry,
                    )
                    else value
                )

                # Set the entry
                self._cache[key] = entry

            else:
                # Update the entry
                entry.update(other=value)

            # Update the last accessed timestamp
            entry.update_last_accessed()

    def __str__(self) -> str:
        """
        Return the string representation of the cache.

        Returns:
            str: The string representation of the cache.
        """

        return self.__repr__()

    @property
    def cache(self) -> dict[str, PebbleCacheEntry]:
        """
        Return the cache.

        Returns:
            dict[str, PebbleCacheEntry]: The cache.
        """

        return dict(self._cache)

    @property
    def cleanup_interval(self) -> int:
        """
        Return the cleanup interval.

        Returns:
            int: The cleanup interval.
        """

        return self._cleanup_interval

    @property
    def last_cleaned_at(self) -> Optional[datetime]:
        """
        Return the last cleaned at timestamp.

        Returns:
            Optional[datetime]: The last cleaned at timestamp.
        """

        return self._last_cleaned_at

    @last_cleaned_at.setter
    def last_cleaned_at(
        self,
        value: datetime,
    ) -> None:
        """
        Set the last cleaned at timestamp.

        Args:
            value (datetime): The last cleaned at timestamp.
        """

        self._last_cleaned_at = value

    @property
    def max_size(self) -> Optional[int]:
        """
        Return the max size.

        Returns:
            Optional[int]: The max size.
        """

        return self._max_size

    @property
    def time_to_live(self) -> int:
        """
        Return the time to live.

        Returns:
            int: The time to live.
        """

        return self._time_to_live

    def _is_expired(
        self,
        key: str,
    ) -> bool:
        """ """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Obtain the entry
            entry: Optional[PebbleCacheEntry] = self._cache.get(key, None)

            # Check if the entry is not None
            if entry is None:
                # Return False if the entry is not found
                return False

            # Return True if the entry is expired, False otherwise
            return entry.is_expired(time_to_live=self._time_to_live)

    def _maybe_cleanup(self) -> None:
        """
        Clean up the cache if it is stale.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Check if the cache is empty
            if self.is_empty():
                # Return if the cache is empty
                return

            # Get the expired keys
            expired_keys: list[str] = [key for key in self._cache if self._is_expired(key=key)]

            # Check if the cache is stale
            if is_stale(
                interval=self._cleanup_interval,
                timestamp=self._last_cleaned_at,
            ):
                # Clean up the cache
                self._cleanup()

            for key in expired_keys:
                # Remove the expired key
                self._cache.pop(key)

            # Update the last cleaned at timestamp
            self._last_cleaned_at = datetime.now()

    def _maybe_evict(self) -> None:
        """
        Evict the cache if it is full.

        Returns:
            None
        """

        # Check if the cache is not full
        if not self.is_full():
            # Return if the cache is not full
            return

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Get all entries, oldest first
            entries: list[tuple[str, PebbleCacheEntry]] = sorted(
                self._cache.items(),
                key=lambda x: x[1].last_accessed or datetime.min,
            )

            # Iterate over the entries
            for key, _ in entries[: len(entries) - self._max_size + 1]:
                # Remove the oldest entry
                self._cache.pop(key)

    def add(
        self,
        key: str,
        value: dict[str, Any],
    ) -> None:
        """
        Add a new entry to the cache.

        Args:
            key (str): The key of the entry to add.
            value (dict[str, Any]): The value of the entry to add.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Check if the key already exists
            if self._cache.get(
                key,
                None,
            ):
                # Update the entry
                self._cache[key].update(other=value)

                # Return early
                return

            # Add the key
            self._cache[key] = PebbleCacheEntry(value)

    def clear(self) -> None:
        """
        Clear the cache.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Clear the cache
            self._cache.clear()

    def delete(
        self,
        key: str,
    ) -> None:
        """
        Delete the entry with the given key.

        Args:
            key (str): The key of the entry to delete.

        Returns:
            None
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Delete the entry
            self._cache.pop(
                key,
                None,
            )

    def filter(
        self,
        string: str,
        flag: Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"] = "CASE_INSENSITIVE",
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> dict[str, Any]:
        """
        Filter the cache.

        Args:
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]): The flag to use for the filter. Defaults to "CASE_INSENSITIVE".
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use for the filter. Defaults to "ALL".
            string (str): The string to filter by.

        Returns:
            dict[str, Any]: The filtered cache.
        """

        # Initialize the filter engine
        filter_engine: PebbleFilterEngine = PebbleFilterEngine(
            table=self.to_dict().get(
                "entries",
                {},
            ),
        )

        # Set the filter
        filter_engine.set_filter(
            filter=PebbleFilterString(
                flag=flag,
                string=string,
            ),
            scope=scope,
        )

        # Return the filtered cache
        return filter_engine.filter()

    def flush_dirty(self) -> dict[str, PebbleCacheEntry]:
        """
        Flush the dirty entries.

        Returns:
            dict[str, PebbleCacheEntry]: The dirty entries.
        """

        # Declare the result
        result: dict[str, PebbleCacheEntry] = {}

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Iterate over the cache
            for key, value in self._cache.items():
                # Check if the entry is dirty
                if value.is_dirty():
                    # Add the entry to the result
                    result[key] = value

        # Return the result
        return result

    def get(
        self,
        key: str,
    ) -> Optional[PebbleCacheEntry]:
        """
        Get the entry with the given key.

        Args:
            key (str): The key of the entry to get.

        Returns:
            Optional[PebbleCacheEntry]: The entry with the given key.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the entry with the given key
            return self._cache.get(
                key,
                None,
            )

    def has(
        self,
        key: str,
    ) -> bool:
        """
        Check if the cache has the given key.

        Args:
            key (str): The key to check.

        Returns:
            bool: True if the cache has the given key, False otherwise.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Check if the cache is empty
            if self.is_empty():
                # Return False if the cache is empty
                return False

            # Return True if the cache has the given key, False otherwise
            return key in self._cache

    def is_empty(self) -> bool:
        """
        Check if the cache is empty.

        Returns:
            bool: True if the cache is empty, False otherwise.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return True if the cache is empty, False otherwise
            return not self._cache

    def is_full(self) -> bool:
        """
        Check if the cache is full.

        Returns:
            bool: True if the cache is full, False otherwise.
        """

        # Check if the max size is None
        if self._max_size is None:
            # Return False if the max size is None
            return False

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return True if the cache is full, False otherwise
            return len(self._cache) >= self._max_size

    def items(self) -> list[tuple[str, PebbleCacheEntry]]:
        """
        Return the items of the cache.

        Returns:
            list[tuple[str, PebbleCacheEntry]]: The items of the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the items of the cache
            return [
                (
                    key,
                    value,
                )
                for (
                    key,
                    value,
                ) in self._cache.items()
                if not self._is_expired(key=key)
            ]

    def keys(self) -> list[str]:
        """
        Return the keys of the cache.

        Returns:
            list[str]: The keys of the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the keys of the cache
            return [key for key in self._cache.keys() if not self._is_expired(key=key)]

    def set(
        self,
        key: str,
        value: PebbleCacheEntry,
    ) -> None:
        """
        Set the entry with the given key.

        Args:
            key (str): The key of the entry to set.
            value (Any): The value of the entry to set.

        Returns:
            None
        """

        # Check if the value is a PebbleCacheEntry
        if not isinstance(
            value,
            PebbleCacheEntry,
        ):
            # Raise a TypeError if the value is not a PebbleCacheEntry
            raise TypeError("value must be a PebbleCacheEntry object.")

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Set the entry
            self._cache[key] = value

    def size(self) -> int:
        """
        Return the size of the cache.

        Returns:
            int: The size of the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the size of the cache
            return len(self._cache)

    def to_dict(self) -> dict[str, Any]:
        """
        Return the dictionary representation of the cache.

        Returns:
            dict[str, Any]: The dictionary representation of the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            return {
                "total": self.size(),
                "entries": {
                    index: entry.data
                    for (
                        index,
                        entry,
                    ) in enumerate(iterable=self._cache.values())
                },
            }

    def update_last_cleaned_at(self) -> None:
        """
        Update the last cleaned at time.

        Returns:
            None
        """

        # Update the last cleaned at time
        self._last_cleaned_at = datetime.now()

    def values(self) -> list[PebbleCacheEntry]:
        """
        Return the values of the cache.

        Returns:
            list[PebbleCacheEntry]: The values of the cache.
        """

        # Acquire a lock to ensure thread safety
        with self._lock:
            # Return the values of the cache
            return [
                value
                for (
                    key,
                    value,
                ) in self._cache.items()
                if not self._is_expired(key=key)
            ]
