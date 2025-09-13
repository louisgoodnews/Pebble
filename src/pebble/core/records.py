"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from collections.abc import Mapping
from typing import Any, Final, Iterator, List, Optional


__all__: Final[List[str]] = ["PebbleRecord"]


class PebbleRecord(Mapping):
    """
    A class to represent a record.
    """

    def __init__(
        self,
        dictionary: dict[str, Any],
    ) -> None:
        """
        Initialize a new PebbleRecord object.

        Args:
            dictionary (dict[str, Any]): The dictionary to store in the record.

        Returns:
            None
        """

        # Store the dictionary in an instance variable
        self._dictionary: Final[dict[str, Any]] = dict(dictionary)

    def __eq__(
        self,
        other: "PebbleRecord",
    ) -> bool:
        """
        Check if the record is equal to another record.

        Args:
            other (PebbleRecord): The other record to compare with.

        Returns:
            bool: True if the records are equal, False otherwise.
        """

        # Check if the other object is a PebbleRecord object
        if not isinstance(
            other,
            PebbleRecord,
        ):
            # Return False if the other object is not a PebbleRecord object
            return False

        # Return True if the records are equal
        return self._dictionary == other._dictionary

    def __contains__(
        self,
        key: Any,
    ) -> bool:
        """
        Check if the record contains a key.

        Args:
            key (Any): The key to check.

        Returns:
            bool: True if the record contains the key, False otherwise.
        """

        return key in self._dictionary

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get the value of a key in the dictionary.

        Args:
            key (str): The key to get.

        Returns:
            Any: The value of the key.
        """

        # Check if the key is in the dictionary
        if key not in self._dictionary:
            # Raise a KeyError if the key is not found
            raise KeyError(key)

        # Return the value of the key
        return self._dictionary[key]

    def __hash__(self) -> int:
        """
        Get the hash of the dictionary.

        Returns:
            int: The hash of the dictionary.
        """

        return hash(frozenset(self._dictionary.items()))

    def __iter__(self) -> Iterator[str]:
        """
        Get an iterator over the keys in the dictionary.

        Returns:
            Iterator[str]: An iterator over the keys in the dictionary.
        """

        return iter(self._dictionary)

    def __len__(self) -> int:
        """
        Get the length of the dictionary.

        Returns:
            int: The length of the dictionary.
        """

        return len(self._dictionary)

    def __repr__(self) -> str:
        """
        Get a string representation of the dictionary.

        Returns:
            str: The string representation of the dictionary.
        """

        return f"<PebbleRecord({self._dictionary!r})>"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> "PebbleRecord":
        """
        Set the value of a key in the dictionary.

        Args:
            key (str): The key to set.
            value (Any): The value to set.

        Returns:
            PebbleRecord: The new PebbleRecord object.

        Raises:
            PebbleRecordImmutabilityViolationError: As the dictionary is immutable.
        """

        raise PebbleRecordImmutabilityViolationError(
            key=key,
            value=value,
        )

    def __str__(self) -> str:
        """
        Get a string representation of the dictionary.

        Returns:
            str: The string representation of the dictionary.
        """

        return str(self._dictionary)

    @property
    def dictionary(self) -> dict[str, Any]:
        """
        Get the dictionary.

        Returns:
            dict[str, Any]: The dictionary.
        """

        return self._dictionary.copy()

    def empty(self) -> bool:
        """
        Check if the dictionary is empty.

        Returns:
            bool: True if the dictionary is empty, False otherwise.
        """

        return not bool(self._dictionary)

    @classmethod
    def from_dict(
        cls,
        dictionary: dict[str, Any],
    ) -> "PebbleRecord":
        """
        Get a new PebbleRecord object from a dictionary.

        Args:
            dictionary (dict[str, Any]): The dictionary to create a PebbleRecord object from.

        Returns:
            PebbleRecord: The PebbleRecord object.
        """

        def process(value: Any) -> Any:
            """
            Process a value.

            This method is used to process a value to ensure that it is a valid value for a PebbleRecord object.

            Args:
                value (Any): The value to process.

            Returns:
                Any: The processed or unprocessed value.
            """

            # Check if the value is a dictionary
            if isinstance(
                value,
                dict,
            ):
                # Process the dictionary
                return PebbleRecord.from_dict(dictionary=value)
            # Check if the value is a list
            elif isinstance(
                value,
                list,
            ):
                # Process the list as a tuple
                return tuple(process(item) for item in value)
            # Check if the value is a set
            elif isinstance(
                value,
                set,
            ):
                # Process the set as a frozenset
                return frozenset(process(item) for item in value)
            # Check if the value is a PebbleRecord
            elif isinstance(
                value,
                PebbleRecord,
            ):
                # Return the value as is
                return value

            # Return the value as is
            return value

        # Return the PebbleRecord object
        return PebbleRecord(
            {
                key: process(value=value)
                for (
                    key,
                    value,
                ) in dictionary.items()
            }
        )

    def get(
        self,
        key: str,
        default: Optional[Any] = None,
    ) -> Any:
        """
        Get the value of a key in the dictionary.

        Args:
            key (str): The key to get.
            default (Optional[Any]): The default value to return if the key is not found.

        Returns:
            Any: The value of the key.
        """

        return self._dictionary.get(
            key,
            default,
        )

    def items(self) -> ItemsView[str, Any]:
        """
        Get the items of the dictionary.

        Returns:
            ItemsView[str, Any]: The items of the dictionary.
        """

        return self._dictionary.items()

    def keys(self) -> KeysView[str]:
        """
        Get the keys of the dictionary.

        Returns:
            KeysView[str]: The keys of the dictionary.
        """

        return self._dictionary.keys()

    def merge(
        self,
        other: "PebbleRecord",
        conflict_resolver: Callable[[Any, Any], Any] = lambda x, y: y,
    ) -> "PebbleRecord":
        """
        Merge another dictionary into this dictionary.

        Args:
            other (PebbleRecord): The dictionary to merge.

        Returns:
            PebbleRecord: The merged dictionary.

        Raises:
            PebbleRecordImmutabilityViolationError: As the dictionary is immutable.
        """

        def deep_merge(
            source: dict[str, Any],
            target: dict[str, Any],
            conflict_resolver: Callable[[Any, Any], Any],
        ) -> dict[str, Any]:
            """
            Merge two dictionaries recursively.

            Args:
                source (dict[str, Any]): The source dictionary.
                target (dict[str, Any]): The target dictionary.

            Returns:
                dict[str, Any]: The merged dictionary.
            """

            # Create a copy of the first dictionary
            result: dict[str, Any] = dict(source)

            # Iterate over the second dictionary
            for (
                key,
                value,
            ) in target.items():
                # Check if the key is in the first dictionary and both values are dictionaries
                if (
                    key in result
                    and isinstance(
                        result[key],
                        dict,
                    )
                    and isinstance(
                        value,
                        dict,
                    )
                ):
                    # Recursively merge the dictionaries
                    result[key] = deep_merge(
                        conflict_resolver=conflict_resolver,
                        source=result[key],
                        target=value,
                    )
                else:
                    # Use the conflict resolver to resolve the conflict
                    result[key] = conflict_resolver(
                        source.get(key),
                        value,
                    )

            # Return the merged dictionary
            return result

        # Return a new PebbleRecord object
        return PebbleRecord(
            dictionary=deep_merge(
                conflict_resolver=conflict_resolver,
                source=self.to_dict(),
                target=other.to_dict(),
            ),
        )

    def set(
        self,
        key: str,
        value: Any,
    ) -> "PebbleRecord":
        """
        Set the value of a key in the dictionary.

        This method is used to set the value of a key in the dictionary.
        It returns a new PebbleRecord object with the updated value.

        Args:
            key (str): The key to set.
            value (Any): The value to set.

        Returns:
            PebbleRecord: The new PebbleRecord object.

        Raises:
            PebbleRecordImmutabilityViolationError: As the dictionary is immutable.
        """

        # Create a copy of the dictionary
        dictionary: dict[str, Any] = self.to_dict()

        # Set the value of the key
        dictionary[key] = value

        # Return a new PebbleRecord object
        return PebbleRecord(dictionary=dictionary)

    def size(self) -> int:
        """
        Get the size of the dictionary.

        Returns:
            int: The size of the dictionary.
        """

        return len(self._dictionary)

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the dictionary to a dictionary.

        This method is used to convert the dictionary to a dictionary.
        It returns a new dictionary with the values of the dictionary.

        Returns:
            dict[str, Any]: The dictionary.
        """

        def process(value) -> Any:
            """
            Process a value.

            This method is used to process a value to ensure that it is a valid value for a dictionary.

            Args:
                value (Any): The value to process.

            Returns:
                Any: The processed or unprocessed value.
            """

            # Check if the value is a PebbleRecord
            if isinstance(
                value,
                PebbleRecord,
            ):
                # Return the dictionary of the PebbleRecord
                return value.to_dict()
            # Check if the value is a frozenset
            elif isinstance(
                value,
                frozenset,
            ):
                # Return the set of the frozenset
                return set(process(item) for item in value)
            # Check if the value is a tuple
            elif isinstance(
                value,
                tuple,
            ):
                # Return the list of the tuple
                return list(process(item) for item in value)
            # Check if the value is a dictionary
            elif isinstance(
                value,
                dict,
            ):
                # Return the dictionary
                return {
                    key: process(value)
                    for (
                        key,
                        value,
                    ) in value.items()
                }
            # Check if the value is a list
            elif isinstance(
                value,
                list,
            ):
                # Return the list of the tuple
                return [process(item) for item in value]
            # Check if the value is a set
            elif isinstance(
                value,
                set,
            ):
                # Return the set
                return {
                    key: process(value)
                    for (
                        key,
                        value,
                    ) in value.items()
                }

            # Return the value as is
            return value

        # Return the dictionary
        return {
            key: process(value)
            for (
                key,
                value,
            ) in self._dictionary.items()
        }

    def to_list(self) -> list[Any]:
        """
        Convert the dictionary to a list.

        This method is used to convert the dictionary to a list.
        It returns a new list with the values of the dictionary.

        Returns:
            list[Any]: The list.
        """

        return list(self._dictionary.values())

    def to_set(self) -> Set[Any]:
        """
        Convert the dictionary to a set.

        This method is used to convert the dictionary to a set.
        It returns a new set with the values of the dictionary.

        Returns:
            set[Any]: The set.
        """

        return set(self._dictionary.values())

    def to_tuple(self) -> tuple[Any]:
        """
        Convert the dictionary to a tuple.

        This method is used to convert the dictionary to a tuple.
        It returns a new tuple with the values of the dictionary.

        Returns:
            tuple[Any]: The tuple.
        """

        return tuple(self._dictionary.values())

    def update(
        self,
        **kwargs,
    ) -> "PebbleRecord":
        """
        Update the dictionary.

        This method is used to update the dictionary.
        It returns a new PebbleRecord object with the updated values.

        Args:
            **kwargs: The key-value pairs to update.

        Returns:
            PebbleRecord: The new PebbleRecord object.

        Raises:
            PebbleRecordImmutabilityViolationError: As the dictionary is immutable.
        """

        # Create a copy of the dictionary
        dictionary: dict[str, Any] = self.to_dict()

        # Update the dictionary
        dictionary.update(kwargs)

        # Return a new PebbleRecord object
        return PebbleRecord(dictionary=dictionary)

    def without(
        self,
        key: str,
        default: Optional[Any] = None,
    ) -> "PebbleRecord":
        """
        Pop the value of a key in the dictionary.

        Args:
            key (str): The key to pop.
            default (Optional[Any]): The default value to return if the key is not found.

        Returns:
            "PebbleRecord": The new dictionary.

        Raises:
            PebbleRecordImmutabilityViolationError: As the dictionary is immutable.
        """

        # Create a copy of the dictionary
        dictionary: dict[str, Any] = self.to_dict()

        # Pop the value of the key
        dictionary.pop(
            key,
            default,
        )

        # Return the new dictionary
        return PebbleRecord(dictionary=dictionary)

    def values(self) -> ValuesView[Any]:
        """
        Get the values of the dictionary.

        Returns:
            ValuesView[Any]: The values of the dictionary.
        """

        return self._dictionary.values()
