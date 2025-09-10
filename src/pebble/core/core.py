"""
Author: Louis Goodnews
Date: 2025-09-05
"""

import uuid

from collections.abc import KeysView, ItemsView, Mapping, ValuesView
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import (
    Any,
    Callable,
    Final,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Set,
    Self,
    Type,
    Union,
)
from uuid import UUID

from utils.constants import (
    COMBINATORS,
    FILTER_PATTERN,
    OBJECT_SIZE_LIMIT,
    OPERATORS,
    QUERY_PATTERN,
    SCOPES,
)

from utils.exceptions import (
    PebbleFieldValidationError,
    PebbleFileWriteError,
    PebbleFilterStringFormatError,
    PebbleFilterStringOperatorError,
    PebbleFilterStringScopeError,
    PebbleQueryStringFormatError,
    PebbleRecordImmutabilityViolationError,
    PebbleSizeExceededError,
    PebbleTableAlreadyRegisteredError,
    PebbleTableNotFoundError,
)
from utils.utils import (
    create_file,
    delete_file,
    dict_to_json,
    find_all_patterns,
    is_uuid,
    json_to_dict,
    match_pattern,
    object_to_string,
    PebbleFieldTypes,
    read_file,
    run_asynchronously,
    string_to_object,
    write_file,
)


__all__: Final[List[str]] = [
    "Pebble",
    "PebbleConstraint",
    "PebbleDatabase",
    "PebbleDatabaseBuilder",
    "PebbleField",
    "PebbleFieldFactory",
    "PebbleFilterString",
    "PebbleModel",
    "PebbleTable",
    "PebbleTableBuilder",
    "PebbleTool",
    "PebbleToolBuilder",
]


class PebbleField:
    """
    A class to represent a field in a PebbleTable.
    """

    def __init__(
        self,
        name: str,
        type_: Union[
            Literal[
                "BOOLEAN",
                "DATE",
                "DATETIME",
                "DECIMAL",
                "DICTIONARY",
                "FLOAT",
                "INTEGER",
                "LIST",
                "PATH",
                "SET",
                "STRING",
                "TIME",
                "TUPLE",
                "UUID",
            ],
            PebbleFieldTypes,
        ],
        choices: Optional[Iterable[Any]] = None,
        default: Optional[Any] = None,
        required: bool = False,
        validator: Optional[Callable[[Any], bool]] = None,
    ) -> None:
        """
        Initialize a new PebbleField object.

        Args:
            choices (Optional[Iterable[Any]]): The choices for the field. Defaults to None.
            default (Optional[Any]): The default value of the field. Defaults to None.
            name (str): The name of the field.
            required (bool): Whether the field is required. Defaults to False.
            type_ (Union[
                Literal[
                    "BOOLEAN",
                    "DATE",
                    "DATETIME",
                    "DECIMAL",
                    "DICTIONARY",
                    "FLOAT",
                    "INTEGER",
                    "LIST",
                    "PATH",
                    "SET",
                    "STRING",
                    "TIME",
                    "TUPLE",
                    "UUID",
                ],
                PebbleFieldTypes,
            ],
            PebbleFieldTypes]): The type of the field.
            validator (Optional[Callable[[Any], bool]]): The validator for the field. Defaults to None.

        Returns:
            None
        """

        # Store the passed choices in an instance variable
        self._choices: Final[Optional[Iterable[Any]]] = choices

        # Store the passed default in an instance variable
        self._default: Final[Optional[Any]] = default

        # Store the passed name in an instance variable
        self._name: Final[str] = name

        # Store the passed required in an instance variable
        self._required: Final[bool] = required

        # Check if the passed type is a valid type
        if not isinstance(type_, PebbleFieldTypes):
            # Convert the passed type to a PebbleFieldTypes
            type_ = PebbleFieldTypes[type_]

        # Store the passed type in an instance variable
        self._type_: Final[PebbleFieldTypes] = type_

        # Store the passed validator in an instance variable
        self._validator: Final[Optional[Callable[[Any], bool]]] = validator

    @property
    def choices(self) -> Optional[Iterable[Any]]:
        """
        Get the choices for the field.

        Returns:
            Optional[Iterable[Any]]: The choices for the field.
        """

        return self._choices

    @property
    def default(self) -> Optional[Any]:
        """
        Get the default value of the field.

        Returns:
            Optional[Any]: The default value of the field.
        """

        # Check if the default value is a mutable type
        if isinstance(self._default, (dict, list, set)):
            # Return a copy of the default value
            return self._default.copy()

        # Return the default value
        return self._default

    @property
    def name(self) -> str:
        """
        Get the name of the field.

        Returns:
            str: The name of the field.
        """

        return self._name

    @property
    def required(self) -> bool:
        """
        Get whether the field is required.

        Returns:
            bool: Whether the field is required.
        """

        return self._required

    @property
    def type_(self) -> PebbleFieldTypes:
        """
        Get the type of the field.

        Returns:
            PebbleFieldTypes: The type of the field.
        """

        return self._type_

    @property
    def validator(self) -> Optional[Callable[[Any], bool]]:
        """
        Get the validator for the field.

        Returns:
            Optional[Callable[[Any], bool]]: The validator for the field.
        """

        return self._validator

    def __eq__(
        self,
        other: "PebbleField",
    ) -> bool:
        """
        Check if the field is equal to another field.

        Args:
            other (PebbleField): The other field to compare with.

        Returns:
            bool: True if the fields are equal, False otherwise.
        """

        # Check if the other object is a PebbleField object
        if not isinstance(
            other,
            PebbleField,
        ):
            # Return False if the other object is not a PebbleField object
            return False

        # Return True if the fields are equal
        return self._name == other._name and self._type_ == other._type_

    def __repr__(self) -> str:
        """
        Get a string representation of the field.

        Returns:
            str: The string representation of the field.
        """

        return f"<PebbleField(name={self._name!r}, type_={self._type_!r}, choices={self._choices!r}, default={self._default!r}, required={self._required!r}, validator={self._validator!r})>"

    def __str__(self) -> str:
        """
        Get a string representation of the field.

        Returns:
            str: The string representation of the field.
        """

        return self.__repr__()

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the field to a dictionary.

        Returns:
            dict[str, Any]: The dictionary representation of the field.
        """

        return {
            "name": self._name,
            "type_": self._type_.value,
            "choices": self._choices,
            "default": str(self._default),
            "required": self._required,
            "validator": str(self._validator),
        }

    def validate(
        self,
        value: Optional[Any] = None,
    ) -> bool:
        """
        Validate the value of the field.

        Args:
            value (Optional[Any]): The value to validate.

        Returns:
            bool: True if the value is valid, False otherwise.
        """

        # Check if the value is None
        if value is None:
            # Set the value to the default
            value = self._default

        # Check if the value is None and the field is required
        if self._required and value is None:
            # Raise a PebbleFieldValidationError if the value is None and the field is required
            raise PebbleFieldValidationError(
                f"The field {self._name} is required. It cannot be None."
            )

        # Check if the value is not in the choices
        if self._choices and value not in self._choices:
            # Raise a PebbleFieldValidationError if the value is not in the choices
            raise PebbleFieldValidationError(
                f"The field {self._name} must be one of {self._choices}."
            )

        # Check if the value is not valid
        if self._validator and not self._validator(value):
            # Raise a PebbleFieldValidationError if the value is not valid
            raise PebbleFieldValidationError(f"The field {self._name} is not valid.")

        # Initialize the types dictionary
        types: dict[str, Type] = {
            "boolean": bool,
            "date": date,
            "datetime": datetime,
            "decimal": (Decimal, float, int),
            "dictionary": dict,
            "float": float,
            "integer": int,
            "list": list,
            "path": Path,
            "set": set,
            "string": str,
            "time": time,
            "tuple": tuple,
            "uuid": UUID,
        }

        # Check if the value is of the correct type
        if not isinstance(value, types[self._type_.value]):
            # Raise a PebbleFieldValidationError if the value is not of the correct type
            raise PebbleFieldValidationError(
                f"The field {self._name} must be of type {self._type_.value}."
            )

        # Check if the passed value is the default
        if self._default is not None and value == self._default:
            # Return True if the value is the default
            return True
        # Check if the passed value is not the default
        elif self._default is not None and value != self._default:
            # Raise a PebbleFieldValidationError if the value is not the default
            raise PebbleFieldValidationError(
                f"The field {self._name} must be {self._default}."
            )

        # Return True if the value is valid
        return True

    def value_to_string(
        self,
        value: Any,
        handler: Optional[Callable[[Any], str]] = None,
    ) -> str:
        """
        Convert a value to a string.

        Args:
            value: The value to convert.

        Returns:
            The string representation of the value.
        """

        # Return the string representation of the value
        return object_to_string(
            handler=handler,
            value=value,
        )


class PebbleConstraint:
    """ """

    pass


class PebbleFieldFactory:
    """
    A factory class for creating PebbleField objects.
    """

    @classmethod
    def create(
        cls,
        name: str,
        type_: Literal[
            "BOOLEAN",
            "DATE",
            "DATETIME",
            "DECIMAL",
            "DICTIONARY",
            "FLOAT",
            "INTEGER",
            "LIST",
            "PATH",
            "SET",
            "STRING",
            "TIME",
            "TUPLE",
            "UUID",
        ],
        choices: Optional[Iterable[Any]] = None,
        default: Optional[Any] = None,
        required: bool = False,
        validator: Optional[Callable[[Any], bool]] = None,
    ) -> "PebbleField":
        """
        Create a new PebbleField object.

        This method is a convenience method for creating a new PebbleField object.

        Args:
            name (str): The name of the field.
            type_ (
                Literal[
                    "BOOLEAN",
                    "DATE",
                    "DATETIME",
                    "DECIMAL",
                    "DICTIONARY",
                    "FLOAT",
                    "INTEGER",
                    "LIST",
                    "PATH",
                    "SET",
                    "STRING",
                    "TIME",
                    "TUPLE",
                    "UUID",
                ]): The type of the field.
            choices (Optional[Iterable[Any]]): The choices for the field. Defaults to None.
            default (Optional[Any]): The default value of the field. Defaults to None.
            required (bool): Whether the field is required. Defaults to False.
            validator (Optional[Callable[[Any], bool]]): The validator for the field. Defaults to None.

        Returns:
            PebbleField: The created PebbleField object.
        """

        # Return a new PebbleField object
        return PebbleField(
            choices=choices,
            default=default,
            name=name,
            required=required,
            type_=PebbleFieldTypes[type_],
            validator=validator,
        )


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

    def __iter__(
        self,
    ) -> Iterator[str]:
        """
        Get an iterator over the keys in the dictionary.

        Returns:
            Iterator[str]: An iterator over the keys in the dictionary.
        """

        return iter(self._dictionary)

    def __len__(
        self,
    ) -> int:
        """
        Get the length of the dictionary.

        Returns:
            int: The length of the dictionary.
        """

        return len(self._dictionary)

    def __repr__(
        self,
    ) -> str:
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

    def __str__(
        self,
    ) -> str:
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


class PebbleFilterString:
    """
    A class to represent a filter string.
    """

    def __init__(
        self,
        string: str,
        flag: Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"] = "CASE_INSENSITIVE",
    ) -> None:
        """
        Initialize a new PebbleFilterString object.

        Args:
            string: The string to store in the filter.

        Returns:
            None
        """

        # Store the field of the filter in an instance variable
        self._field: str = ""

        # Store the flag of the filter in an instance variable
        self._flag: Final[Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]] = flag

        # Store the parsed state of the filter in an instance variable
        self._parsed: bool = False

        # Store the operator of the filter in an instance variable
        self._operator: str = ""

        # Store the scope of the filter in an instance variable
        self._scope: str = ""

        # Store the passed string in an instance variable
        self._string: Final[str] = string

        # Store the value of the filter in an instance variable
        self._value: Any = ""

        # Parse the filter string
        self.parse()

    @property
    def field(self) -> str:
        """
        Get the field of the filter.

        Returns:
            The field of the filter.
        """

        return self._field

    @property
    def flag(self) -> Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]:
        """
        Get the flag of the filter.

        Returns:
            The flag of the filter.
        """

        return self._flag

    @property
    def operator(self) -> str:
        """
        Get the operator of the filter.

        Returns:
            The operator of the filter.
        """

        return self._operator

    @property
    def parsed(self) -> bool:
        """
        Get the parsed state of the filter.

        Returns:
            The parsed state of the filter.
        """

        return self._parsed

    @property
    def scope(self) -> str:
        """
        Get the scope of the filter.

        Returns:
            The scope of the filter.
        """

        return self._scope

    @property
    def string(self) -> str:
        """
        Get the string of the filter.

        Returns:
            The string of the filter.
        """

        return self._string

    @property
    def value(self) -> Any:
        """
        Get the value of the filter.

        Returns:
            The value of the filter.
        """

        return self._value

    def __eq__(
        self,
        other: "PebbleFilterString",
    ) -> bool:
        """
        Check if the filter is equal to another object.

        Args:
            other: The object to compare to.

        Returns:
            True if the filter is equal to the other object, False otherwise.
        """

        # Check if the other object is a PebbleFilterString
        if not isinstance(
            other,
            PebbleFilterString,
        ):
            # Return False if the other object is not a PebbleFilterString
            return False

        # Return True if the strings are equal
        return self._string == other.string

    def __repr__(self) -> str:
        """
        Get a string representation of the filter.

        Returns:
            A string representation of the filter.
        """

        return f"<{self.__class__.__name__}(field={self._field!r}, flag={self._flag!r}, operator={self._operator!r}, scope={self._scope!r}, value={self._value!r})>"

    def __str__(self) -> str:
        """
        Get the string of the filter.

        Returns:
            The string of the filter.
        """

        return self._string

    def evaluate(
        self,
        entry: dict[str, Any],
    ) -> bool:
        """
        Evaluate the filter against a single entry (row).

        Args:
            entry: The table entry (dict) to test.

        Returns:
            True if the filter matches the entry, False otherwise.
        """

        # Field not present → no match
        if self._field not in entry:
            # Return False if the field is not present
            return False

        # Get the value of the field
        entry_value: Optional[Any] = entry.get(
            self._field,
            None,
        )

        # Check if the field is present
        if not entry_value:
            # Return False if the field is not present
            return False

        # Get the operator
        operator: Literal[
            "==",
            "!=",
            "<",
            ">",
            "<=",
            ">=",
            "in",
            "not in",
            "is",
            "is not",
        ] = self._operator.lower()

        # Get the value
        value: Any = self._value

        # Convert to lowercase if the flag is CASE_INSENSITIVE
        if self._flag == "CASE_INSENSITIVE":
            # Convert entry_value to lowercase if it is a string
            entry_value = entry_value.lower()

            # Convert value to lowercase if it is a string
            value = value.lower()

        # Comparison logic
        if operator == "==":
            # Return True if the values are equal
            return entry_value == value
        elif operator == "!=":
            # Return True if the values are not equal
            return entry_value != value
        elif operator == "<":
            # Return True if the entry value is less than the value
            return entry_value < value
        elif operator == ">":
            # Return True if the entry value is greater than the value
            return entry_value > value
        elif operator == "<=":
            # Return True if the entry value is less than or equal to the value
            return entry_value <= value
        elif operator == ">=":
            # Return True if the entry value is greater than or equal to the value
            return entry_value >= value
        elif operator == "in":
            # Return True if the entry value is in the value
            return entry_value in value
        elif operator == "not in":
            # Return True if the entry value is not in the value
            return entry_value not in value
        elif operator == "is":
            # Return True if the entry value is the value
            return entry_value is value
        elif operator == "is not":
            # Return True if the entry value is not the value
            return entry_value is not value

        # Unknown operator
        raise ValueError(f"Unsupported operator: {self._operator}")

    def parse(self) -> None:
        """
        Parse the filter string.

        Returns:
            None
        """

        # Check if the filter has already been parsed
        if self._parsed:
            # Return if the filter has already been parsed
            return

        # Parse the filter string
        parsed: Optional[dict[str, Any]] = match_pattern(
            pattern=FILTER_PATTERN,
            string=self._string,
        )

        # Check if the string is in the correct format
        if parsed is None:
            # Raise a PebbleFilterStringFormatError if the string is not in the correct format
            raise PebbleFilterStringFormatError(string=self._string)

        # Set the attributes of the filter
        for (
            key,
            value,
        ) in parsed.items():
            # Set the attribute of the filter
            setattr(
                self,
                f"_{key}",
                string_to_object(string=value),
            )

        # Set the parsed state of the filter to True
        self._parsed = True

    def parts(self) -> list[str]:
        """
        Get the parts of the filter string.

        Returns:
            The parts of the filter string.
        """

        return [
            self._table,
            self._field,
            self._scope,
            self._operator,
            self._value,
        ]

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the filter to a dictionary.

        Returns:
            The filter as a dictionary.
        """

        return {
            "field": self._field,
            "operator": self._operator,
            "scope": self._scope,
            "string": self._string,
            "table": self._table,
            "value": {
                "type": type(self._value).__name__,
                "value": self._value,
            },
        }

    def to_list(self) -> list[str]:
        """
        Convert the filter to a list.

        This method is a wrapper for the parts() method.

        Returns:
            The filter as a list.
        """

        return self.parts()

    def to_str(self) -> str:
        """
        Convert the filter to a string.

        Returns:
            The filter as a string.
        """

        return self._string

    def to_tuple(self) -> tuple[str, str, str, str, str]:
        """
        Convert the filter to a tuple.

        Returns:
            The filter as a tuple.
        """

        return tuple(self.parts())


class PebbleFilterEngine:
    """
    A filter engine for filtering tables.
    """

    def __init__(
        self,
        table: Union[dict[str, Any], "PebbleTable"],
    ) -> None:
        """
        Initialize a new PebbleFilterEngine object.

        Args:
            table (Union[dict[str, Any], PebbleTable]): The dictionary or table to filter.

        Returns:
            None
        """

        # Initialize an empty dictionary of filters
        self._filters: dict[
            str,
            dict[
                str,
                Union[
                    Literal["ALL", "ANY", "NONE"],
                    Literal["AND", "OR"],
                    PebbleFilterString,
                ],
            ],
        ] = {}

        # Store the passed table in an instance variable
        self._table: Union[dict[str, Any], PebbleTable] = table

    @property
    def filters(
        self,
    ) -> list[
        dict[
            str,
            Union[
                Literal["ALL", "ANY", "NONE"],
                Literal["AND", "OR"],
                PebbleFilterString,
            ],
        ],
    ]:
        """
        Get the filters of the engine.

        Returns:
            list[dict[str, Union[Literal["ALL", "ANY", "NONE"], Literal["AND", "OR"], PebbleFilterString]]]: The filters of the engine.
        """

        return list(self._filters.values())

    @property
    def table(self) -> Union[dict[str, Any], "PebbleTable"]:
        """
        Get the table of the engine.

        Returns:
            Union[dict[str, Any], "PebbleTable"]: The table of the engine.
        """

        return self._table

    @table.setter
    def table(
        self,
        value: Union[dict[str, Any], PebbleTable],
    ) -> None:
        """
        Set the table of the engine.

        Args:
            value (Union[dict[str, Any], PebbleTable]): The table to set.

        Returns:
            None
        """

        # Check if the value is a PebbleTable
        if isinstance(
            value,
            PebbleTable,
        ):
            # Set the table to the entries of the table
            value = PebbleTable.entries

        # Update the instance variable with the passed value
        self._table = value

    def __repr__(self) -> str:
        """
        Get a string representation of the engine.

        Returns:
            A string representation of the engine.
        """

        return f"<{self.__class__.__name__}(filters={self._filters!r}, table={self._table!r})>"

    def __str__(self) -> str:
        """
        Get a string representation of the engine.

        Returns:
            A string representation of the engine.
        """

        return self.__repr__()

    def filter(self) -> dict[str, Any]:
        """
        Filter the table.

        Returns:
            The filtered table.
        """

        table: list[dict[str, Any]] = []

        # Check if the table is a PebbleTable
        if isinstance(
            self._table,
            PebbleTable,
        ):
            # Get the table entries
            table = self._table.all(format="list")
        else:
            # Set the table to the passed table
            table = PebbleTool.to_list(dictionary=self._table)

        # Initialize an empty dictionary
        result: dict[str, Any] = {
            "filter": f"{'.'.join([f'{str(filter.get('filter', ''))}.{filter.get('operator', '')}.{filter.get('scope', '')}' for filter in self.filters])}",
            "total": 0,
            "values": [],
        }

        # Iterate over the table entries
        for entry in table:

            # Initialize an empty list of matches
            matches: list[bool] = []

            # Initialize the operator
            outer_operator: Literal["AND", "OR", ""] = ""

            # Initialize the scope
            outer_scope: Literal["ALL", "ANY", "NONE", ""] = ""

            # Iterate over the filters
            for clause in self.filters:
                # Get the filter string of the clause
                filter_string: PebbleFilterString = clause["filter"]

                # Get the scope of the clause
                outer_scope = clause.get("scope", "ALL")

                # Get the (merge) operator of the clause
                outer_operator = clause.get("operator", "AND")

                # Initialize a match variable
                match: bool = filter_string.evaluate(entry=entry)

                # Invert the match if the scope is NONE
                if outer_scope == "NONE":
                    # Invert the match
                    match = not match

                # Append the match to the list
                matches.append(match)

            # Check if the entry matches any of the filters
            if matches:
                # Check if the entry matches all of the filters
                if outer_operator == "AND" and all(matches):
                    # Add the entry to the result
                    result["values"].append(entry)

                    # Increment the total
                    result["total"] += 1
                # Check if the entry matches any of the filters
                elif outer_operator == "OR" and any(matches):
                    # Add the entry to the result
                    result["values"].append(entry)

                    # Increment the total
                    result["total"] += 1

        # Return the result
        return result

    def set_filter(
        self,
        filter: PebbleFilterString,
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> "PebbleFilterEngine":
        """
        Set the filter of the engine.

        Args:
            filter: The filter to set.
            scope: The scope to use when combining the filters.

        Returns:
            The engine.
        """

        # Add the filter to the dictionary
        self._filters[filter.to_str()] = {
            "filter": filter,
            "scope": scope,
        }

        # Return the engine
        return self

    def set_filters(
        self,
        filters: list[PebbleFilterString],
        operator: Literal["AND", "OR"] = "AND",
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> "PebbleFilterEngine":
        """
        Set the filters of the engine.

        Args:
            filters (list[PebbleFilterString]): The filters to set.
            operator (Literal["AND", "OR"]): The operator to use when combining the filters.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use when combining the filters.

        Returns:
            PebbleFilterEngine: The engine.
        """

        # Iterate over the filters
        for filter in filters:
            # Add the filters to the dictionary
            self._filters[filter.to_str()] = {
                "filter": filter,
                "operator": operator,
                "scope": scope,
            }

        # Return the engine
        return self


class PebbleTable:
    """
    A table in a database.
    """

    def __init__(
        self,
        name: str,
        constraints: Optional[dict[str, Any]] = None,
        definition: Optional[dict[str, Any]] = None,
        entries: Optional[dict[str, Any]] = None,
        fields: Optional[dict[str, Any]] = None,
        identifier: Optional[str] = None,
        indexes: Optional[dict[str, Any]] = None,
        primary_key: Optional[dict[str, Any]] = None,
        references: Optional[dict[str, Any]] = None,
        unique: Optional[dict[str, Any]] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Initialize a new PebbleTable object.

        Args:
            constraints: The dictionary to store the constraints of the table.
            definition: The dictionary to store the definition of the table.
            entries: The dictionary to store the table data in.
            fields: The dictionary to store the fields of the table.
            identifier: The identifier of the table.
            indexes: The dictionary to store the indexes of the table.
            name: The name of the table.
            path: The path to the table file.
            primary_key: The dictionary to store the primary key of the table.
            references: The dictionary to store the references of the table.
            unique: The dictionary to store the unique constraints of the table.

        Returns:
            None
        """

        # Check if the constraints is None
        if constraints is None:
            # Initialize an empty constraints
            constraints = {}

        # Store the passed constraints in an instance variable
        self._constraints: dict[str, Any] = constraints

        # Check if the definition is None
        if definition is None:
            # Initialize an empty definition
            definition = {}

        # Store the passed definition in an instance variable
        self._definition: dict[str, Any] = definition

        # Check if the entries is None
        if entries is None:
            # Initialize an empty entries
            entries = {
                "total": 0,
                "values": {},
            }

        # Store the passed entries in an instance variable
        self._entries: dict[str, Any] = entries

        # Check if the fields is None
        if fields is None:
            # Initialize an empty fields
            fields = {}

        # Store the passed fields in an instance variable
        self._fields: dict[str, Any] = fields

        # Check if the identifier is None
        if identifier is None:
            # Initialize an empty identifier
            identifier = uuid.uuid4().hex

        # Store the passed identifier in an instance variable
        self._identifier: Final[str] = identifier

        # Check if the indexes is None
        if indexes is None:
            # Initialize an empty indexes
            indexes = {}

        # Store the passed indexes in an instance variable
        self._indexes: dict[str, Any] = indexes

        # Store the passed name string in an instance variable
        self._name: str = name

        # Check if the path is a string
        if path is not None or not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path)

        # Store the passed path in an instance variable
        self._path: Path = path

        # Check if the primary_key is None
        if primary_key is None:
            # Initialize an empty primary_key
            primary_key = {}

        # Store the passed primary_key in an instance variable
        self._primary_key: dict[str, Any] = primary_key

        # Check if the references is None
        if references is None:
            # Initialize an empty references
            references = {}

        # Store the passed references in an instance variable
        self._references: dict[str, Any] = references

        # Check if the unique is None
        if unique is None:
            # Initialize an empty unique
            unique = {}

        # Store the passed unique in an instance variable
        self._unique: dict[str, Any] = unique

    @property
    def constraints(self) -> dict[str, Any]:
        """
        Get the constraints of the table.

        Returns:
            dict[str, Any]: The constraints of the table.
        """

        return dict(self._constraints)

    @property
    def definition(self) -> dict[str, Any]:
        """
        Get the definition of the table.

        Returns:
            dict[str, Any]: The definition of the table.
        """

        return {
            "constraints": self._constraints,
            "fields": self._fields,
            "indexes": self._indexes,
            "primary_key": self._primary_key,
            "references": self._references,
            "unique": self._unique,
        }

    @property
    def entries(self) -> dict[str, Any]:
        """
        Get the entries of the table.

        Returns:
            dict[str, Any]: The entries of the table.
        """

        return dict(self._entries)

    @property
    def fields(self) -> dict[str, Any]:
        """
        Get the fields of the table.

        Returns:
            dict[str, Any]: The fields of the table.
        """

        return dict(self._fields)

    @property
    def identifier(self) -> str:
        """
        Get the identifier of the table.

        Returns:
            str: The identifier of the table.
        """

        return self._identifier

    @property
    def indexes(self) -> dict[str, Any]:
        """
        Get the indexes of the table.

        Returns:
            dict[str, Any]: The indexes of the table.
        """

        return dict(self._indexes)

    @property
    def name(self) -> str:
        """
        Get the name of the table.

        Returns:
            str: The name of the table.
        """

        return self._name

    @name.setter
    def name(
        self,
        value: str,
    ) -> None:
        """
        Set the name of the table.

        Args:
            value: The name of the table.

        Returns:
            None
        """

        # Update the instance variable with the passed value
        self._name = value

    @property
    def path(self) -> Path:
        """
        Get the path of the table.

        Returns:
            Path: The path of the table.
        """

        return self._path

    @path.setter
    def path(
        self,
        value: Union[Path, str],
    ) -> None:
        """
        Set the path of the table.

        Args:
            value: The path of the table.

        Returns:
            None
        """

        # Check if the value is a string
        if not isinstance(
            value,
            Path,
        ):
            # Convert the string to a Path object
            value = Path(value)

        # Update the instance variable with the passed value
        self._path = value

    @property
    def primary_key(self) -> dict[str, Any]:
        """
        Get the primary key of the table.

        Returns:
            dict[str, Any]: The primary key of the table.
        """

        return dict(self._primary_key)

    @property
    def references(self) -> dict[str, Any]:
        """
        Get the references of the table.

        Returns:
            dict[str, Any]: The references of the table.
        """

        return dict(self._references)

    @property
    def unique(self) -> dict[str, Any]:
        """
        Get the unique constraints of the table.

        Returns:
            dict[str, Any]: The unique constraints of the table.
        """

        return dict(self._unique)

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the table contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the table contains the key, False otherwise.
        """

        # Check if the key is in the dictionary
        return key in [values for values in self._entries["values"].values()]

    def __eq__(
        self,
        other: object,
    ) -> bool:
        """
        Check if the table is equal to another table.

        Args:
            other: The other table to compare with.

        Returns:
            True if the tables are equal, False otherwise.
        """

        # Check if the other object is a PebbleTable object
        if not isinstance(
            other,
            PebbleTable,
        ):
            # Return False if the other object is not a PebbleTable object
            return False

        # Return True if the tables are equal
        return self._identifier == other._identifier

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get an item from the table itself or its data.

        This method is used to access the table object's data.
        It can also be used to access the table object's instance variables.

        Args:
            key: The key of the item.

        Returns:
            The item.
        """

        # Return the value from the dictionary
        return self._entries.get(key)

    def __repr__(self) -> str:
        """
        Get a string representation of the table.

        Returns:
            A string representation of the table.
        """

        return f"<{self.__class__.__name__}(definition=[{self._definition}], entries=[{self._entries['total']} {'entry' if self._entries['total'] == 1 else 'entries'}], identifier=[{self._identifier}], name=[{self._name}], path=[{self._path}])>"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set an item in the table itself or its data.

        This method is used to set the table object's data.
        It can also be used to set the table object's instance variables.

        Args:
            key: The key of the item.
            value: The value of the item.

        Returns:
            None
        """

        # Check if the key is the identifier
        if key == "identifier":
            # Raise an AttributeError if the identifier is set
            raise AttributeError(
                f"Cannot set 'identifier' of a {self.__class__.__name__} object. As this attribute is immutable."
            )

        # Set the value in the dictionary
        self._entries[key] = value

    def all(
        self, format: Literal["dict", "list", "set", "tuple"] = "dict"
    ) -> Union[
        dict[str, Any], list[dict[str, Any]], set[dict[str, Any]], tuple[dict[str, Any]]
    ]:
        """
        Get all the data in the table.

        This method will return all the data in the table.
        It can do so in different formats.
        Supported formats are:
            - dict
            - list
            - set
            - tuple

        Args:
            format (Literal["dict", "list", "set", "tuple"]): The format of the data.

        Returns:
            Union[dict[str, Any], list[dict[str, Any]], set[dict[str, Any]], tuple[dict[str, Any]]]: The data in the table.
        """

        # Get a copy of the table's entries
        result: dict[str, Any] = dict(self._entries["values"])

        # Check if the format is dict
        if format == "dict":
            # Return the result
            return result
        # Check if the format is list
        elif format == "list":
            # Return a list of the table's entries
            return list(result.values())
        # Check if the format is set
        elif format == "set":
            # Return a set of the table's entries
            return set(result.values())
        # Check if the format is tuple
        elif format == "tuple":
            # Return a tuple of the table's entries
            return tuple(result.values())

        # Unknown format
        raise ValueError(f"Unsupported format: {format}")

    def check_for_size(self) -> bool:
        """
        Check if the table exceeds the maximum size.

        Returns:
            bool: True if the table exceeds the maximum size, False otherwise.

        Raises:
            PebbleSizeExceededError: If the table exceeds the maximum size.
        """

        # Check if the table exceeds the maximum size
        if self.get_size_of() > OBJECT_SIZE_LIMIT:
            # Raise a PebbleSizeExceededError
            raise PebbleSizeExceededError(name=self._name)

        # Return False if the table does not exceed the maximum size
        return False

    def commit(self) -> None:
        """
        Commit the table to a file.

        This method will commit the table to a file.

        Returns:
            None
        """

        # Check if the file exists
        if not self.path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=self.path,
            )

        # Write the dictionary to the file
        run_asynchronously(
            function=write_file,
            path=self.path,
            data=dict_to_json(dictionary=self.to_dict()),
        )

    def configure(
        self,
        path: str,
        value: Any,
    ) -> None:
        """
        Configure the table.

        This method will configure the table.

        Args:
            path: The path to the value.
            value: The value to set.

        Returns:
            None
        """

        # Check if the path does not start with "constraints" or "definition"
        if not path.startswith("constraints") and not path.startswith("definition"):
            # Raise a ValueError
            raise ValueError(
                "Cannot configure non-constraints or non-definition values with 'configure' method. PebbleTable object is otherwise immutable."
            )

        # Create a builder
        builder: PebbleToolBuilder = PebbleToolBuilder(dictionary=self.to_dict())

        # Set the value
        builder.set(path=path, value=value)

        # Update the table object
        self.__dict__.update(builder.build().to_dict())

    def delete(self) -> None:
        """
        Delete the table file.

        Returns:
            None
        """

        # Check if the file exists
        if not self._path.exists():
            # Return if the file does not exist
            return

        # Delete the table file
        delete_file(file=self.path)

        # Delete the table object
        del self

    @classmethod
    def from_file(
        cls,
        path: Union[Path, str],
    ) -> "PebbleTable":
        """
        Create a new PebbleTable object from a file.

        Args:
            path: The path to the table file.

        Returns:
            A new PebbleTable object.
        """

        # Check if the path is a string
        if not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path)

        # Check if the file exists
        if not path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=path,
            )

        json: dict[str, Any] = json_to_dict(
            run_asynchronously(
                function=read_file,
                path=path,
            )
        )

        # Return the table
        return PebbleTable(
            definition=json.get("definition", {}),
            entries=json.get("entries", {}),
            identifier=json.get("identifier", ""),
            name=json.get("name", ""),
            path=json.get("path", ""),
        )

    @classmethod
    def from_dict(
        cls,
        dictionary: dict[str, Any],
        name: str,
    ) -> "PebbleTable":
        """
        Create a new PebbleTable object from a dictionary.

        Args:
            dictionary: The dictionary to create the table from.
            name: The name of the table.

        Returns:
            A new PebbleTable object.
        """

        # Return the table
        return PebbleTable(
            definition=dictionary,
            name=name,
        )

    def get_by_id(
        self,
        identifier: int,
    ) -> Any:
        """
        Get an item from the table by its identifier.

        Args:
            identifier: The identifier of the item.

        Returns:
            The item.
        """

        return self._entries.get(
            identifier,
            None,
        )

    def get_by_ids(
        self,
        identifiers: list[int],
    ) -> dict[int, Any]:
        """
        Get items from the table by their identifiers.

        Args:
            identifiers: The identifiers of the items.

        Returns:
            The items.
        """

        # Return the items
        return {
            identifier: self._entries.get(
                identifier,
                None,
            )
            for identifier in identifiers
        }

    def get_size_of(self) -> int:
        """
        Get the size of the table.

        Returns:
            int: The size of the table.
        """

        # Return the size of the table
        return self._entries["total"]

    def remove(
        self,
        identifier: Union[int, str],
    ) -> bool:
        """
        Delete an entry from the table.

        Args:
            identifier: The identifier of the entry to delete.

        Returns:
            True if the entry was deleted, False otherwise.
        """

        # Check if the identifier is an integer
        if not isinstance(
            identifier,
            int,
        ):
            # Convert the identifier to a string
            identifier = str(identifier)

        # Check if the identifier exists
        if identifier not in self._entries["values"]:
            # Return False if the identifier does not exist
            return False

        # Delete the identifier
        self._entries["values"].pop(identifier)

        # Return True if the identifier was deleted
        return True

    def set(
        self,
        entry: dict[str, Any],
        identifier: Optional[str] = None,
    ) -> int:
        """
        Set an item in the table.

        Args:
            entry: The entry to set.
            identifier: The identifier of the entry.

        Returns:
            The identifier of the entry.
        """

        # Check if the total is in the dictionary
        if "total" not in self._entries:
            # Set the total to 0
            self._entries["total"] = 0

        # Check if the values is in the dictionary
        if "values" not in self._entries:
            # Set the values to an empty dictionary
            self._entries["values"] = {}

        # Check if the identifier is None
        if identifier is None:
            # Generate a new identifier
            identifier = str(len(self._entries["values"]))

            # Increment the total
            self._entries["total"] += 1

        # Set the entry in the dictionary
        self._entries["values"][identifier] = entry

        # Return the identifier of the entry
        return identifier

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the table to a dictionary.

        Returns:
            The table as a dictionary.
        """

        # Return the table as a dictionary
        return {
            "definition": {
                "constraints": self._constraints,
                "fields": self._fields,
                "indexes": self._indexes,
                "primary_key": self._primary_key,
                "references": self._references,
                "unique": self._unique,
            },
            "entries": self._entries,
            "identifier": self._identifier,
            "name": self._name,
            "path": self._path,
        }

    def to_json(self) -> str:
        """
        Convert the table to a JSON string.

        Returns:
            The table as a JSON string.
        """

        # Return the table as a JSON string
        return dict_to_json(dictionary=self.to_dict())

    def to_str(self) -> str:
        """
        Convert the table to a string.

        Returns:
            The table as a string.
        """

        # Return the table as a string
        return str(self._entries)


class PebbleTableFactory:
    """
    A factory class for creating new PebbleTable objects.
    """

    @classmethod
    def create(
        cls,
        name: str,
        definition: Optional[dict[str, Any]] = None,
        entries: Optional[dict[str, Any]] = None,
        identifier: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> PebbleTable:
        """
        Create a new PebbleTable object.

        Args:
            name: The name of the table.
            definition: The dictionary to store the definition of the table.
            entries: The dictionary to store the table data in.
            identifier: The identifier of the table.
            path: The path to the table file.

        Returns:
            A new PebbleTable object.
        """

        # Return the table
        return PebbleTable(
            name=name,
            definition=definition,
            entries=entries,
            identifier=identifier,
            path=path,
        )


class PebbleTableBuilder:
    """
    Build a new PebbleTable object.
    """

    def __init__(self) -> None:
        """
        Initialize a new PebbleTableBuilder object.

        Returns:
            None
        """

        # Store the configuration in an instance variable
        self._configuration: dict[str, Any] = {
            "definition": {
                "constraints": {},
                "fields": {},
                "indexes": {},
                "primary_key": {},
                "references": {},
                "unique": {},
            },
            "entries": {
                "total": 0,
                "values": {},
            },
            "identifier": "",
            "name": "",
            "path": "",
        }

    @property
    def configuration(self) -> dict[str, Any]:
        """
        Get the configuration.

        Returns:
            dict[str, Any]: The configuration.
        """

        return dict(self._configuration)

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the configuration contains a key.

        Args:
            key (str): The key to check for.

        Returns:
            bool: True if the configuration contains the key, False otherwise.
        """

        # Return the presence of the key in the configuration
        return key in self._configuration

    def __eq__(
        self,
        other: "PebbleTableBuilder",
    ) -> bool:
        """
        Check if the builder is equal to another builder.

        Args:
            other (PebbleTableBuilder): The other builder to compare to.

        Returns:
            bool: True if the builders are equal, False otherwise.
        """

        # Check if the other object is a PebbleTableBuilder
        if not isinstance(
            other,
            PebbleTableBuilder,
        ):
            # Return False
            return False

        # Return the equality of the builders
        return self._configuration == other._configuration

    def __repr__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            str: The string representation of the builder.
        """

        # Return the string representation of the builder
        return f"PebbleTableBuilder(configuration={self._configuration})"

    def __str__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            str: The string representation of the builder.
        """

        # Return the string representation of the builder
        return str(self._configuration)

    def build(self) -> PebbleTable:
        """
        Build the table.

        Returns:
            PebbleTable: The table.
        """

        # Create and return the table
        return PebbleTableFactory.create(**self._configuration)

    def with_constraint(
        self,
        value: PebbleConstraint,
    ) -> Self:
        """
        Set the constraint.

        Args:
            value (PebbleConstraint): The constraint to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"constraints": {}}

        # Set the constraint
        self._configuration["definition"]["constraints"][value.name] = value.to_dict()

        # Return the builder
        return self

    def with_definition(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the definition.

        Args:
            value (dict[str, Any]): The definition to set.

        Returns:
            Self: The builder.
        """

        # Set the definition
        self._configuration["definition"] = value

        # Return the builder
        return self

    def with_dictionary(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the dictionary.

        Args:
            value (dict[str, Any]): The dictionary to set.

        Returns:
            Self: The builder.
        """

        # Set the dictionary
        self._configuration["dictionary"] = value

        # Return the builder
        return self

    def with_field(
        self,
        value: PebbleField,
    ) -> Self:
        """
        Set the field.

        Args:
            value (PebbleField): The field to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"fields": {}}

        # Set the field
        self._configuration["definition"]["fields"][value.name] = value.to_dict()

        # Return the builder
        return self

    def with_identifier(
        self,
        value: Optional[str] = None,
    ) -> Self:
        """
        Set the identifier.

        Args:
            value (str): The identifier to set.

        Returns:
            Self: The builder.
        """

        # Generate a random UUID if the value is None
        if value is None:
            # Generate a random UUID
            value = uuid.uuid4().hex

        # Check if the value is a valid UUID
        if not is_uuid(string=value):
            # Raise a ValueError
            raise ValueError("Invalid UUID identifier")

        # Set the identifier
        self._configuration["identifier"] = value

        # Return the builder
        return self

    def with_name(
        self,
        value: str,
    ) -> Self:
        """
        Set the name.

        Args:
            value (str): The name to set.

        Returns:
            Self: The builder.
        """

        # Set the name
        self._configuration["name"] = value

        # Return the builder
        return self

    def with_path(
        self,
        value: Union[Path, str],
    ) -> Self:
        """
        Set the path.

        Args:
            value (Union[Path, str]): The path to set.

        Returns:
            Self: The builder.
        """

        # Check if the value is a Path object
        if not isinstance(
            value,
            Path,
        ):
            # Convert the string to a Path object
            value = Path(value)

        # Set the path
        self._configuration["path"] = value

        # Return the builder
        return self

    def with_primary_key(
        self,
        value: str,
    ) -> Self:
        """
        Set the primary key.

        Args:
            value (str): The primary key to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"primary_key": {}}

        # Set the primary key
        self._configuration["definition"]["primary_key"] = value

        # Return the builder
        return self

    def with_reference(
        self,
        value: str,
    ) -> Self:
        """
        Set the reference.

        Args:
            value (str): The reference to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"reference": {}}

        # Generate an identifier for the reference
        identifier: str = str(len(self._configuration["definition"]["reference"]))

        # Set the reference
        self._configuration["definition"]["reference"][identifier] = value

        # Return the builder
        return self

    def with_references(
        self,
        value: list[str],
    ) -> Self:
        """
        Set the references.

        Args:
            value (list[str]): The references to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"references": {}}

        # Set the references
        for reference in value:
            # Check if the reference already exists
            if reference in self._configuration["definition"]["references"].values():
                # Raise a ValueError
                raise ValueError(f"Reference '{reference}' already exists")

            # Generate an identifier for the reference
            identifier: str = str(len(self._configuration["definition"]["references"]))

            # Set the reference
            self._configuration["definition"]["references"][identifier] = reference

        # Return the builder
        return self

    def with_unique_key(
        self,
        value: str,
    ) -> Self:
        """
        Set the unique key.

        Args:
            value (str): The unique key to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"unique": {}}

        # Generate an identifier for the unique key
        identifier: str = str(len(self._configuration["definition"]["unique"]))

        # Set the unique key
        self._configuration["definition"]["unique"][identifier] = value

        # Return the builder
        return self

    def with_unique_keys(
        self,
        value: list[str],
    ) -> Self:
        """
        Set the unique keys.

        Args:
            value (list[str]): The unique keys to set.

        Returns:
            Self: The builder.
        """

        # Check if the definition is not in the configuration
        if "definition" not in self._configuration:
            # Initialize the definition as a nested dictionary
            self._configuration["definition"] = {"unique": {}}

        # Set the unique keys
        for key in value:
            # Check if the key already exists
            if key in self._configuration["definition"]["unique"].values():
                # Raise a ValueError
                raise ValueError(f"Unique key '{key}' already exists")

            # Generate an identifier for the unique key
            identifier: str = str(len(self._configuration["definition"]["unique"]))

            # Set the unique key
            self._configuration["definition"]["unique"][identifier] = key

        # Return the builder
        return self


class PebbleQueryString:
    """
    A class to represent a query string.
    """

    def __init__(
        self,
        string: str,
        flag: Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"] = "CASE_INSENSITIVE",
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> None:
        """
        Initialize a new PebbleQueryString object.

        Args:
            flag (Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.
            string (str): The string to store.

        Returns:
            None
        """

        # Initialize the engine as an instance variable
        self._engine: PebbleFilterEngine = PebbleFilterEngine(table={})

        # Initialize the filters dictionary as an instance variable
        self._filters: dict[str, list[PebbleFilterString]] = {}

        # Initialize the flag as an instance variable
        self._flag: Final[Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]] = flag

        # Initialize the parsed state as an instance variable
        self._parsed: bool = False

        # Initialize the scope as an instance variable
        self._scope: Final[Literal["ALL", "ANY", "NONE"]] = scope

        # Store the passed string str as an instance variable
        self._string: Final[str] = string

        # Initialize the queries list as an instance variable
        self._sub_queries: list[tuple[str, Optional[str]]] = []

        # Initialize the tables list as an instance variable
        self._tables: list[str] = []

        # Parse the string
        self.parse()

    @property
    def filters(self) -> dict[str, list[PebbleFilterString]]:
        """
        Return the filters.

        Returns:
            dict[str, list[PebbleFilterString]]: The filters.
        """

        return dict(self._filters)

    @property
    def flag(self) -> Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]:
        """
        Return the flag.

        Returns:
            Literal["CASE_SENSITIVE", "CASE_INSENSITIVE"]: The flag.
        """

        return self._flag

    @property
    def scope(self) -> Literal["ALL", "ANY", "NONE"]:
        """
        Return the scope.

        Returns:
            Literal["ALL", "ANY", "NONE"]: The scope.
        """

        return self._scope

    @property
    def string(self) -> str:
        """
        Return the string.

        Returns:
            str: The string.
        """

        return self._string

    @property
    def sub_queries(self) -> list[tuple[str, Optional[str]]]:
        """
        Return the sub-queries.

        Returns:
            list[tuple[str, Optional[str]]]: The sub-queries.
        """

        return list(self._sub_queries)

    @property
    def tables(self) -> list[str]:
        """
        Return the tables.

        Returns:
            list[str]: The tables.
        """

        return list(self._tables)

    def __contains__(
        self,
        item: str,
    ) -> bool:
        """
        Check if the string contains the item.

        Args:
            item (str): The item to check.

        Returns:
            bool: True if the string contains the item, False otherwise.
        """

        return item in self._string

    def __eq__(
        self,
        other: "PebbleQueryString",
    ) -> bool:
        """
        Check if the string is equal to the other string.

        Args:
            other (PebbleQueryString): The other string to compare.

        Returns:
            bool: True if the string is equal to the other string, False otherwise.
        """

        return self._string == other._string

    def __hash__(self) -> int:
        """
        Return the hash of the string.

        Returns:
            int: The hash of the string.
        """

        return hash(self._string)

    def __repr__(self) -> str:
        """
        Return the string representation of the string.

        Returns:
            str: The string representation of the string.
        """

        return f"<{self.__class__.__name__}(string={self._string!r})>"

    def __str__(self) -> str:
        """
        Return the string representation of the string.

        Returns:
            str: The string representation of the string.
        """

        return self._string

    def evaluate(
        self,
        table: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Evaluate the string.

        Args:
            table (dict[str, Any]): The table to evaluate.

        Returns:
            dict[str, Any]: The result of the evaluation.
        """

        # Initialize an empty dictionary
        result: dict[str, Any] = {
            "filter": ".".join(
                f"{f.field}.{f.operator}.{f.scope}"
                for filters in self._filters.values()
                for f in filters
            ),
            "total": 0,
            "values": [],
        }

        # Set the table of the engine
        self._engine.table = table

        # Iterate over the filters
        for filter_list in self._filters.values():
            # Set the filters of the engine
            self._engine.set_filters(filters=filter_list)

        # Filter the table and set the values
        result["values"] = self._engine.filter()

        # Set the total
        result["total"] = len(result["values"])

        # Return the result
        return result

    def parse(self) -> None:
        """
        Parse the string.

        This method attempts to split the string into queries based on the combinators.
        If the string is not split, it is considered as a single query.

        Returns:
            None
        """

        # Check if the string has already been parsed
        if self._parsed:
            # Return if the string has already been parsed
            return

        # Parse the string into queries
        self.parse_queries()

        # Iterate over the queries
        for string in self._queries:
            # Parse the string into filters
            self.parse_filters(string=string)

        # Set the parsed state of the string to True
        self._parsed = True

    def parse_filters(
        self,
        string: str,
    ) -> None:
        """
        Parse the string into filters.

        Args:
            string (str): The string to parse.

        Returns:
            None
        """

        try:
            # Initialize the filter string
            filter_string: PebbleFilterString = PebbleFilterString(
                flag=self._flag,
                string=string,
            )

            # Check if the table is not in the filters
            if filter_string.table not in self._filters:
                # Initialize the table filters
                self._filters[filter_string.table] = []

            # Set the filter
            self._filters[filter_string.table].append(filter_string)
        except PebbleFilterStringFormatError:
            # Skip if the filter string is not in the correct format
            pass

    def parse_queries(self) -> None:
        """
        Parse the string into queries.

        This method attempts to split the string into queries based on the combinators.
        If the string is not split, it is considered as a single query.

        Returns:
            None
        """

        # Check if the string has already been parsed
        if self._parsed:
            # Return if the string has already been parsed
            return

        # Find all patterns in the string
        parsed: Optional[list[dict[str, Optional[str]]]] = find_all_patterns(
            pattern=QUERY_PATTERN,
            string=self._string,
        )

        # Check if the string is in the correct format
        if parsed is None:
            # Raise a PebbleQueryStringFormatError if the string is not in the correct format
            raise PebbleQueryStringFormatError(string=self._string)

        # Add the tables to the tables list
        self._tables.extend(set([query.get("table", "") for query in parsed]))

        # Set the queries
        self._sub_queries.extend(parsed)

    def parts(self) -> list[list[str]]:
        """
        Return the parts of the string.

        Returns:
            list[list[str]]: The parts of the string.
        """

        return [filter.parts() for filter in self._filters.values()]

    def to_dict(self) -> dict[str, dict[str, Any]]:
        """
        Return the dictionary representation of the string.

        Returns:
            dict[str, dict[str, Any]]: The dictionary representation of the string.
        """

        return {
            filter.table: filter.to_dict()
            for filter in [filter for filter in self._filters.values()]
        }

    def to_list(self) -> list[list[str]]:
        """
        Return the list representation of the string.

        Returns:
            list[list[str]]: The list representation of the string.
        """

        return [
            filter.to_list() for filter in [filter for filter in self._filters.values()]
        ]

    def to_str(self) -> str:
        """
        Return the string representation of the string.

        Returns:
            str: The string representation of the string.
        """

        return self._string

    def to_tuple(self) -> tuple[tuple[str]]:
        """
        Return the tuple representation of the string.

        Returns:
            tuple[tuple[str]]: The tuple representation of the string.
        """

        return tuple(
            filter.to_tuple()
            for filter in [filter for filter in self._filters.values()]
        )


class PebbleQueryEngine:
    """
    A class to represent a query engine.
    """

    def __init__(
        self,
        database: Union[dict[str, Any], "PebbleDatabase"],
    ) -> None:
        """
        Initialize a new PebbleQueryEngine object.

        Args:
            database (Union[dict[str, Any], PebbleDatabase]): The database to query.

        Returns:
            None
        """

        # Store the passed database PebbleDatabase as an instance variable
        self._database: Union[dict[str, Any], PebbleDatabase] = database

        # Initialize the filters list as an instance variable
        self._filters: list[PebbleQueryString] = []

    def query(self) -> list[dict[str, Any]]:
        """
        Return the query results.

        Returns:
            list[dict[str, Any]]: The query results.
        """

        # Initialize an empty list
        results: list[dict[str, Any]] = []

        # Iterate over the filters
        for filter in self._filters:
            # Evaluate the filter
            results.extend(filter.evaluate(table=self._database))

        # Return the results
        return results

    def set_query(
        self,
        string: str,
        flag: Literal[
            "CASE_INSENSITIVE",
            "CASE_SENSITIVE",
        ] = "CASE_INSENSITIVE",
        scope: Literal[
            "ALL",
            "ANY",
            "NONE",
        ] = "ALL",
    ) -> "PebbleQueryEngine":
        """
        Set the query.

        Args:
            string (str): The string to set the query from.
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.

        Returns:
            PebbleQueryEngine: The query engine object.
        """

        # Initialize a new PebbleQueryString object
        query_string: PebbleQueryString = PebbleQueryString(
            string=string,
            flag=flag,
            scope=scope,
        )

        # Add the query string to the filters list
        self._filters.append(query_string)

        # Return the query engine object
        return self

    def set_queries(
        self,
        strings: list[str],
        flag: Literal[
            "CASE_INSENSITIVE",
            "CASE_SENSITIVE",
        ] = "CASE_INSENSITIVE",
        scope: Literal[
            "ALL",
            "ANY",
            "NONE",
        ] = "ALL",
    ) -> "PebbleQueryEngine":
        """
        Set the queries.

        Args:
            strings (list[str]): The strings to set the queries from.
            flag (Literal["CASE_INSENSITIVE", "CASE_SENSITIVE"]: The flag to use.
            scope (Literal["ALL", "ANY", "NONE"]: The scope to use.

        Returns:
            PebbleQueryEngine: The query engine object.
        """

        # Iterate over the strings
        for string in strings:
            # Initialize a new PebbleQueryString object
            query_string: PebbleQueryString = PebbleQueryString(
                string=string,
                flag=flag,
                scope=scope,
            )

            # Add the query string to the filters list
            self._filters.append(query_string)

        # Return the query engine object
        return self


class PebbleDatabase:
    """
    A class to represent a database.
    """

    def __init__(
        self,
        name: str,
        tables: Optional[dict[str, Any]] = None,
        identifier: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Initialize a new PebbleTable object.

        Args:
            name (str): The name of the table.
            tables (dict[str, Any]): The dictionary to store the table data in.
            identifier (str): The identifier of the table.
            path (Union[Path, str]): The path to the table file.

        Returns:
            None
        """

        # Check if the tables is None
        if tables is None:
            # Initialize an empty dictionary
            tables = {
                "total": 0,
                "values": {},
            }

        # Store the passed dictionary in an instance variable
        self._tables: dict[str, Any] = tables

        # Check if the identifier is None
        if identifier is None:
            # Initialize an empty identifier
            identifier = uuid.uuid4().hex

        # Store the passed identifier in an instance variable
        self._identifier: Final[str] = identifier

        # Store the passed name string in an instance variable
        self._name: str = name

        # Check if the path is a string
        if path is not None or not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path or ".")

        # Store the passed path in an instance variable
        self._path: Path = path

    @property
    def identifier(self) -> str:
        """
        Get the identifier of the database.

        Returns:
            str: The identifier of the database.
        """

        return self._identifier

    @property
    def name(self) -> str:
        """
        Get the name of the database.

        Returns:
            str: The name of the database.
        """

        return self._name

    @name.setter
    def name(
        self,
        value: str,
    ) -> None:
        """
        Set the name of the database.

        Args:
            value: The name of the database.

        Returns:
            None
        """

        # Update the instance variable with the passed value
        self._name = value

    @property
    def path(self) -> Path:
        """
        Get the path of the database.

        Returns:
            Path: The path of the database.
        """

        return self._path

    @path.setter
    def path(
        self,
        value: Union[Path, str],
    ) -> None:
        """
        Set the path of the database.

        Args:
            value: The path of the database.

        Returns:
            None
        """

        # Check if the value is a string
        if not isinstance(
            value,
            Path,
        ):
            # Convert the string to a Path object
            value = Path(value)

        # Update the instance variable with the passed value
        self._path = value

    @property
    def tables(self) -> dict[str, Any]:
        """
        Get the tables of the database.

        Returns:
            dict[str, Any]: The tables of the database.
        """

        return dict(self._tables)

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the database contains a key.

        Args:
            key: The key to check.

        Returns:
            True if the database contains the key, False otherwise.
        """

        return key in [value["name"] for value in self._tables["values"].values()]

    def __eq__(
        self,
        other: object,
    ) -> bool:
        """
        Check if the database is equal to another database.

        Args:
            other: The other database to compare with.

        Returns:
            True if the databases are equal, False otherwise.
        """

        # Check if the other object is a PebbleDatabase object
        if not isinstance(
            other,
            PebbleDatabase,
        ):
            # Return False if the other object is not a PebbleDatabase object
            return False

        # Return True if the databases are equal
        return self._identifier == other._identifier

    def __getitem__(
        self,
        key: str,
    ) -> Any:
        """
        Get an item from the database itself or its data.

        This method is used to access the database object's data.
        It can also be used to access the database object's instance variables.

        Args:
            key: The key of the item.

        Returns:
            The item.
        """

        # Return the value from the dictionary
        return self._tables["values"].get(key)

    def __repr__(self) -> str:
        """
        Get a string representation of the database.

        Returns:
            A string representation of the database.
        """

        return f"<{self.__class__.__name__}(tables=[{self._tables['total']} {'table' if self._tables['total'] == 1 else 'tables'}], identifier=[{self._identifier}], name=[{self._name}], path=[{self._path}])>"

    def __setitem__(
        self,
        key: str,
        value: Any,
    ) -> None:
        """
        Set an item in the database itself or its data.

        This method is used to set the database object's data.
        It can also be used to set the database object's instance variables.

        Args:
            key: The key of the item.
            value: The value of the item.

        Returns:
            None
        """

        # Check if the key is the identifier
        if key == "identifier":
            # Raise an AttributeError if the identifier is set
            raise AttributeError(
                f"Cannot set 'identifier' of a {self.__class__.__name__} object. As this attribute is immutable."
            )

        # Set the value in the dictionary
        self._tables[key] = value

    def add_table(
        self,
        table: PebbleTable,
    ) -> None:
        """
        Add a table to the database.

        Args:
            table: The table to add.

        Returns:
            None

        Raises:
            PebbleTableAlreadyRegisteredError: If the table is already registered.
        """

        # Check if the table is already registered
        if table.name in self._tables["values"]:
            # Raise a KeyError if the table is already registered
            raise PebbleTableAlreadyRegisteredError(table=table.name)

        # Check if the total is in the dictionary
        if "total" not in self._tables:
            # Set the total to 0
            self._tables["total"] = 0

        # Check if the values is in the dictionary
        if "values" not in self._tables:
            # Set the values to an empty dictionary
            self._tables["values"] = {}

        # Increment the total
        self._tables["total"] += 1

        # Get the identifier
        identifier: str = str(len(self._tables["values"]))

        # Add the table to the dictionary
        self._tables["values"][identifier] = {
            "name": table.name,
            "path": table.path,
        }

    def all(self) -> dict[str, Any]:
        """
        Get all the data in the database.

        Returns:
            The data in the database.
        """

        return dict(self._tables)

    def check_for_size(self) -> bool:
        """
        Check if the database exceeds the maximum size.

        Returns:
            bool: True if the database exceeds the maximum size, False otherwise.

        Raises:
            PebbleSizeExceededError: If the database exceeds the maximum size.
        """

        # Check if the database exceeds the maximum size
        if self.get_size_of() > OBJECT_SIZE_LIMIT:
            # Raise a PebbleSizeExceededError
            raise PebbleSizeExceededError(name=self._name)

        # Return False if the database does not exceed the maximum size
        return False

    def commit(self) -> None:
        """
        Commit the database to a file.

        This method will commit the database to a file.

        Returns:
            None
        """

        # Check if the file exists
        if not self.path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=self.path,
            )

        # Write the dictionary to the file
        run_asynchronously(
            function=write_file,
            path=self.path,
            data=dict_to_json(dictionary=self.to_dict()),
        )

    def configure(
        self,
        path: str,
        value: Any,
    ) -> None:
        """
        Configure the database.

        This method will configure the database.

        Args:
            path: The path to the value.
            value: The value to set.

        Returns:
            None
        """

        # Check if the path does not start with "constraints" or "definition"
        if not path.startswith("constraints") and not path.startswith("definition"):
            # Raise a ValueError
            raise ValueError(
                "Cannot configure non-constraints or non-definition values with 'configure' method. PebbleDatabase object is otherwise immutable."
            )

        # Create a builder
        builder: PebbleToolBuilder = PebbleToolBuilder(dictionary=self.to_dict())

        # Set the value
        builder.set(path=path, value=value)

        # Update the table object
        self.__dict__.update(builder.build().to_dict())

    def delete(self) -> None:
        """
        Delete the database file and all its tables.

        This method will delete the database file and all its tables.

        Returns:
            None
        """

        # Iterate over the dictionary
        for value in self._tables["values"].values():
            # Delete the table
            PebbleTable.from_file(path=value["path"]).delete()

        # Check if the file exists
        if not self._path.exists():
            # Return if the file does not exist
            return

        # Delete the database file
        delete_file(file=self.path)

        # Delete the database object
        del self

    @classmethod
    def from_file(
        cls,
        path: Union[Path, str],
    ) -> "PebbleDatabase":
        """
        Create a new PebbleDatabase object from a file.

        Args:
            path: The path to the database file.

        Returns:
            A new PebbleDatabase object.
        """

        # Check if the path is a string
        if not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path)

        # Check if the file exists
        if not path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=path,
            )

        json: dict[str, Any] = json_to_dict(
            run_asynchronously(
                function=read_file,
                path=path,
            )
        )

        # Return the database
        return PebbleDatabase(
            identifier=json.get("identifier", ""),
            name=json.get("name", ""),
            path=json.get("path", ""),
            tables=json,
        )

    @classmethod
    def from_dict(
        cls,
        dictionary: dict[str, Any],
        name: str,
    ) -> "PebbleDatabase":
        """
        Create a new PebbleDatabase object from a dictionary.

        Args:
            dictionary: The dictionary to create the database from.
            name: The name of the database.

        Returns:
            A new PebbleDatabase object.
        """

        # Return the database
        return PebbleDatabase(
            name=name,
            dictionary=dictionary,
        )

    def get_size_of(self) -> int:
        """
        Get the size of the database.

        Returns:
            int: The size of the database.
        """

        # Return the size of the database
        return self._tables["total"]

    def remove_table(
        self,
        table: PebbleTable,
    ) -> None:
        """
        Remove a table from the database.

        Args:
            table: The table to remove.

        Returns:
            None

        Raises:
            PebbleTableNotFoundError: If the table is not found.
        """

        # Check if the table is in the dictionary
        if table.name not in self._tables["values"]:
            # Raise a KeyError if the table is not found
            raise PebbleTableNotFoundError(table=table.name)

        # Iterate over the dictionary
        for (
            key,
            value,
        ) in self._tables["values"].items():
            # Check if the table is in the dictionary
            if value["name"] != table.name:
                # Skip the table
                continue

            # Remove the table from the dictionary
            self._tables["values"].pop(key)

            # Decrement the total
            self._tables["total"] -= 1

            # Break the loop
            break

    def table(
        self,
        name: str,
        path: Optional[Path] = None,
    ) -> PebbleTable:
        """
        Get or create a table.

        Args:
            name (str): The name of the table.
            path (Optional[Path], optional): The path to the table. Defaults to None.

        Returns:
            PebbleTable: The table.
        """

        # Check if the table exists
        if name in self._tables:
            # Return the table
            return self._tables[name]

        # Create the table builder
        builder: PebbleTableBuilder = PebbleTableBuilder()

        # Set the identifier
        builder.with_identifier()

        # Set the name
        builder.with_name(value=name)

        # Check if the path is None
        if not path:
            # Configure the path
            path = Path(
                ".",
                f"{name}.json",
            )

        # Set the path
        builder.with_path(value=path)

        # Build the table
        table: PebbleTable = builder.build()

        # Add the table to the database
        self.add_table(table=table)

        # Return the table
        return table

    def tables(
        self,
        names: bool = False,
    ) -> List[PebbleTable]:
        """
        Get all the tables in the database.

        Args:
            names (bool, optional): Whether to return the names of the tables.

        Returns:
            List[PebbleTable]: The tables in the database.
        """

        # Initialize the tables list
        tables: List[PebbleTable] = []

        # Iterate over the dictionary
        for table in self._tables["values"].values():
            # Check if the names flag is set
            if names:
                # Append the name of the table
                tables.append(table["name"])

                # Continue to the next table
                continue

            # Create the table
            tables.append(PebbleTable.from_file(path=table["path"]))

        # Return the tables
        return tables

    def to_dict(self) -> dict[str, Any]:
        """
        Convert the database to a dictionary.

        Returns:
            The dictionary representation of the database.
        """

        return {
            "identifier": self._identifier,
            "name": self._name,
            "path": self._path,
            "tables": self._tables,
        }

    def to_str(self) -> str:
        """
        Convert the database to a string.

        Returns:
            The string representation of the database.
        """

        return dict_to_json(dictionary=self.to_dict())


class PebbleDatabaseFactory:
    """
    A factory class for creating new databases.
    """

    @staticmethod
    def create(
        name: str,
        identifier: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
        tables: Optional[dict[str, Any]] = None,
    ) -> PebbleDatabase:
        """
        Create a new database.

        Args:
            tables (dict[str, Any]): The tables to create the database from.
            identifier (str): The identifier of the database.
            name (str): The name of the database.
            path (Union[Path, str]): The path to the database file.

        Returns:
            PebbleDatabase: The new database.
        """

        # Return the database
        return PebbleDatabase(
            identifier=identifier,
            name=name,
            path=path,
            tables=tables,
        )


class PebbleDatabaseBuilder:
    """
    A builder class for creating new databases.
    """

    def __init__(self) -> None:
        """
        Initialize a new PebbleDatabaseBuilder object.

        Returns:
            None
        """

        # Initialize the configuration
        self._configuration: dict[str, Any] = {
            "identifier": "",
            "name": "",
            "path": "",
            "tables": {
                "total": 0,
                "values": {},
            },
        }

    @property
    def configuration(self) -> dict[str, Any]:
        """
        Get the configuration.

        This method returns a copy of the configuration.

        Returns:
            The configuration.
        """

        # Return the configuration
        return dict(self._configuration)

    def __contains__(
        self,
        key: str,
    ) -> bool:
        """
        Check if the configuration contains a key.

        Args:
            key (str): The key to check for.

        Returns:
            bool: True if the configuration contains the key, False otherwise.
        """

        # Return the presence of the key in the configuration
        return key in self._configuration

    def __eq__(
        self,
        other: "PebbleDatabaseBuilder",
    ) -> bool:
        """
        Check if the builder is equal to another builder.

        Args:
            other (PebbleDatabaseBuilder): The other builder to compare to.

        Returns:
            bool: True if the builders are equal, False otherwise.
        """

        # Check if the other object is a PebbleDatabaseBuilder
        if not isinstance(
            other,
            PebbleDatabaseBuilder,
        ):
            # Return False
            return False

        # Return the equality of the builders
        return self._configuration == other._configuration

    def __repr__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            The string representation of the builder.
        """

        # Return the string representation of the builder
        return f"PebbleDatabaseBuilder(configuration={self._configuration})"

    def __str__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            The string representation of the builder.
        """

        # Return the string representation of the builder
        return str(self._configuration)

    def build(self) -> PebbleDatabase:
        """
        Build the database.

        This method returns a new PebbleDatabase object.

        Returns:
            The database.
        """

        # Return the database
        return PebbleDatabaseFactory.create(**self._configuration)

    def with_dictionary(
        self,
        value: dict[str, Any],
    ) -> Self:
        """
        Set the dictionary of the database.

        Args:
            value (dict[str, Any]): The dictionary of the database.

        Returns:
            Self: The builder.
        """

        # Set the dictionary
        self._configuration["dictionary"] = value

        # Return the builder
        return self

    def with_identifier(
        self,
        value: Optional[str] = None,
    ) -> Self:
        """
        Set the identifier of the database.

        Args:
            value (str): The identifier of the database.

        Returns:
            Self: The builder.
        """

        # Generate a random UUID if the value is None
        if value is None:
            # Generate a random UUID
            value = uuid.uuid4().hex

        # Check if the value is a valid UUID
        if not is_uuid(string=value):
            # Raise a ValueError
            raise ValueError("Invalid UUID identifier")

        # Set the identifier
        self._configuration["identifier"] = value

        # Return the builder
        return self

    def with_name(
        self,
        value: str,
    ) -> Self:
        """
        Set the name of the database.

        Args:
            value (str): The name of the database.

        Returns:
            Self: The builder.
        """

        # Set the name
        self._configuration["name"] = value

        # Return the builder
        return self

    def with_path(
        self,
        value: Union[Path, str],
    ) -> Self:
        """
        Set the path of the database.

        Args:
            value (Union[Path, str]): The path of the database.

        Returns:
            Self: The builder.
        """

        # Check if the value is a Path object
        if not isinstance(value, Path):
            # Convert the string to a Path object
            value = Path(value)

        # Set the path
        self._configuration["path"] = value

        # Return the builder
        return self

    def with_table(
        self,
        value: PebbleTable,
    ) -> Self:
        """
        Set the table of the database.

        Args:
            value (PebbleTable): The table of the database.

        Returns:
            Self: The builder.
        """

        # Check if the tables are not in the configuration
        if "tables" not in self._configuration:
            # Initialize the tables nested dictionary
            self._configuration["tables"] = {"total": 0, "values": {}}

        # Generate an identifier for the table
        identifier: str = str(len(self._configuration["tables"]["values"]))

        # Set the table
        self._configuration["tables"]["values"][identifier] = value

        # Increment the total number of tables
        self._configuration["tables"]["total"] += 1

        # Return the builder
        return self

    def with_tables(
        self,
        value: list[PebbleTable],
    ) -> Self:
        """
        Set the tables of the database.

        Args:
            value (list[PebbleTable]): The tables of the database.

        Returns:
            Self: The builder.
        """

        # Iterate over the tables
        for table in value:
            # Add the table
            self.with_table(value=table)

        # Return the builder
        return self


class PebbleTool:
    """
    A collection of tools for working with Pebble objects.
    """

    @staticmethod
    def from_json(
        file_or_str: Union[bytes, Path, str],
        from_file: bool = True,
    ) -> dict[str, Any]:
        """
        Convert a JSON string to a dictionary.

        Args:
            file_or_str: The JSON string or file to convert.
            from_file: Whether the JSON string is a file.

        Returns:
            The dictionary representation of the JSON string.
        """

        # Check if the JSON string is a file
        if from_file:
            # Check if the file is a Path object
            if not isinstance(
                file_or_str,
                Path,
            ):
                # Convert the string to a Path object
                file_or_str = Path(file_or_str)

            # Read the file
            content: str = run_asynchronously(
                function=read_file,
                path=file_or_str,
            )

            # Convert the JSON string to a dictionary
            return json_to_dict(json_string=content)
        else:
            # Convert the JSON string to a dictionary
            return json_to_dict(json_string=file_or_str)

    @staticmethod
    def immutable(dictionary: dict[str, Any]) -> PebbleRecord:
        """
        Convert a dictionary to an immutable PebbleRecord.

        Args:
            dictionary: The dictionary to convert.

        Returns:
            The immutable PebbleRecord.
        """
        return PebbleRecord(dictionary=dictionary)

    @staticmethod
    def mutable(dictionary: PebbleRecord) -> dict[str, Any]:
        """
        Convert a PebbleRecord to a mutable dictionary.

        This method is a wrapper for the PebbleRecord.to_dict() method.
        Might as well just use the to_dict() method directly.

        Args:
            dictionary: The PebbleRecord to convert.

        Returns:
            The mutable dictionary.
        """

        # Return the dictionary
        return dictionary.to_dict()

    @staticmethod
    def merge(
        source: dict[str, Any],
        target: dict[str, Any],
        conflict_resolver: Callable[[Any, Any], Any] = lambda x, y: y,
    ) -> dict[str, Any]:
        """
        Merge two dictionaries into one.

        This method converts the dictionaries to PebbleRecords and then merges them.

        Args:
            source: The source dictionary.
            target: The target dictionary.
            conflict_resolver: The conflict resolver.

        Returns:
            The merged dictionary.
        """

        # Return the merged dictionary
        return PebbleTool.immutable(dictionary=source).merge(
            conflict_resolver=conflict_resolver,
            other=PebbleTool.immutable(dictionary=target),
        )

    @staticmethod
    def to_json(
        dictionary: dict[str, Any],
        path: Optional[Union[Path, str]] = None,
    ) -> str:
        """
        Convert a dictionary to a JSON string.

        Args:
            dictionary (dict[str, Any]): The dictionary to convert.
            path (Optional[Union[Path, str]]): The path to the file to write the JSON string to.

        Returns:
            str: The path to the file.
        """

        # Check if the path is a string
        if not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path)

        # Check if the file exists
        if not path.exists():
            # Create the file
            run_asynchronously(
                function=create_file,
                path=path,
            )

        # Write the dictionary to the file
        result: bool = run_asynchronously(
            data=dict_to_json(dictionary=dictionary),
            function=write_file,
            path=path,
        )

        # Check if the file was written
        if not result:
            # Raise a PebbleFileWriteError
            raise PebbleFileWriteError(path=path)

        # Return the file path
        return path.resolve().as_posix()

    @staticmethod
    def to_list(dictionary: dict) -> list:
        """
        Convert a dictionary to a list.

        Args:
            dictionary: The dictionary to convert.

        Returns:
            The list representation of the dictionary.
        """

        # Return the list
        return list(dictionary.values())

    @staticmethod
    def traverse(
        dictionary: Union[dict, PebbleDatabase, PebbleRecord, PebbleTable],
        path: str = "",
    ) -> Iterable[tuple[str, Any]]:
        """
        Traverses a dictionary and returns a list of (key, value) pairs.

        Args:
            dictionary: The dictionary to traverse.
            path: The path to the current key. It has to have a format like "key1.key2.key3".

        Returns:
            A list of (key, value) pairs.
        """

        # Check if the dictionary is a PebbleDatabase, PebbleRecord, or PebbleTable
        if isinstance(
            dictionary,
            (
                PebbleDatabase,
                PebbleRecord,
                PebbleTable,
            ),
        ):
            # Convert the PebbleDatabase, PebbleRecord, or PebbleTable to a dictionary
            dictionary = dictionary.to_dict()

        # Iterate over the dictionary
        for (
            key,
            value,
        ) in dictionary.items():
            # Get the current path
            current_path: str = f"{path}.{key}" if path else key

            # Check if the value is a dictionary
            if isinstance(
                value,
                dict,
            ):
                # Recursively traverse the dictionary
                yield from PebbleTool.traverse(value, current_path)
            else:
                # Yield the key and value
                yield (current_path, value)


class PebbleToolBuilder:
    """
    A class that provides a builder pattern for dictionaries.
    """

    def __init__(
        self,
        dictionary: dict[str, Any],
    ) -> None:
        """
        Initialize a new PebbleToolBuilder instance.

        Args:
            dictionary: The dictionary to build.
        """

        # Store the passed dictionary in an instance variable
        self._dictionary: PebbleRecord = PebbleRecord.from_dict(dictionary=dictionary)

        # Initialize the result to None
        self._result: Optional[Union[dict[str, Any], PebbleRecord]] = None

    @property
    def dictionary(self) -> dict[str, Any]:
        """
        Get the dictionary of this PebbleToolBuilder instance.

        Returns:
            dict[str, Any]: The dictionary of this PebbleToolBuilder instance.
        """

        return self._dictionary.to_dict()

    def __repr__(self) -> str:
        """
        Get a string representation of the PebbleToolBuilder instance.

        Returns:
            str: The string representation of the PebbleToolBuilder instance.
        """

        return f"<{self.__class__.__name__}(dictionary={self._dictionary})>"

    def build(self) -> Union[dict[str, Any], PebbleRecord]:
        """
        Build the dictionary.

        Returns:
            Union[dict[str, Any], PebbleRecord]: The built dictionary.
        """

        # Return the result
        return self._result

    def delete(
        self,
        path: str,
    ) -> Self:
        """
        Delete a key from the dictionary.

        Args:
            path (str): The path to the key.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            self._dictionary = self._dictionary.to_dict()

        # Split the path into parts
        parts: list[str] = path.split(".")

        # Set the current dictionary to the dictionary
        current: dict[str, Any] = self._dictionary

        # Iterate over the parts of the path
        for part in parts:
            # Check if the part is not in the current dictionary or if it is not a dictionary
            if part not in current or not isinstance(current[part], dict):
                # Create the dictionary
                current[part] = {}

            # Set the current dictionary to the part
            current = current[part]

        # Delete the key
        current.pop(parts[-1])

        # Set the dictionary to the dictionary
        self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def filter(
        self,
        string: str,
        scope: Literal["ALL", "ANY", "NONE"] = "ALL",
    ) -> Self:
        """
        Filter the dictionary.

        Args:
            scope (Literal["ALL", "ANY", "NONE"]): The scope of the filter. Defaults to "ALL".
            string (str): The filter string.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            dictionary: dict[str, Any] = self._dictionary.to_dict()
        else:
            # Set the dictionary to the passed dictionary
            dictionary = self._dictionary

        # Initialize the filter engine
        engine: PebbleFilterEngine = PebbleFilterEngine(table=dictionary)

        # Set the filter
        engine.set_filter(
            filter=PebbleFilterString(string=string),
            scope=scope,
        )

        # Set the dictionary to the filtered dictionary
        self._dictionary = PebbleRecord.from_dict(dictionary=engine.filter())

        # Return the PebbleToolBuilder
        return self

    def merge(
        self,
        other: PebbleRecord,
    ) -> Self:
        """
        Merge another dictionary into this dictionary.

        Args:
            other (PebbleRecord): The dictionary to merge.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        if not isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a PebbleRecord
            self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Merge the dictionaries
        self._dictionary = self._dictionary.merge(other=other)

        # Return the PebbleToolBuilder
        return self

    def query(
        self,
        string: str,
    ) -> Self:
        """
        Query the dictionary.

        Args:
            string (str): The query string.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        raise NotImplementedError(
            f"{self.__class__.__name__}.query() is not implemented yet."
        )

    def set(
        self,
        path: str,
        value: Any,
    ) -> Self:
        """
        Set a value in the dictionary.

        Args:
            path (str): The path to the value.
            value (Any): The value to set.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            self._dictionary = self._dictionary.to_dict()

        # Split the path into parts
        parts: list[str] = path.split(".")

        # Set the current dictionary to the dictionary
        current: dict[str, Any] = self._dictionary

        # Iterate over the parts of the path
        for part in parts:
            # Check if the part is not in the current dictionary or if it is not a dictionary
            if part not in current or not isinstance(current[part], dict):
                # Create the dictionary
                current[part] = {}

            # Set the current dictionary to the part
            current = current[part]

        # Set the value
        current[parts[-1]] = value

        # Set the dictionary to the dictionary
        self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def to_immutable(self) -> Self:
        """
        Convert the dictionary to an immutable PebbleRecord.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is not a PebbleRecord (i.e. its a dict)
        if not isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a PebbleRecord
            self._dictionary = PebbleRecord.from_dict(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def to_json(self) -> Self:
        """
        Convert the dictionary to a JSON string.

        Returns:
            Self: The PebbleToolBuilder instance.
        """

        # Check if the dictionary is a PebbleRecord
        if isinstance(
            self._dictionary,
            PebbleRecord,
        ):
            # Convert the dictionary to a dictionary
            self._dictionary = self._dictionary.to_dict()

        # Convert the dictionary to a JSON string
        self._result = dict_to_json(dictionary=self._dictionary)

        # Return the PebbleToolBuilder
        return self

    def traverse(self) -> List[tuple[str, Any]]:
        """
        Traverses the dictionary and returns a list of (key, value) pairs.

        Returns:
            List[tuple[str, Any]]: A list of (key, value) pairs.
        """

        # Return the list of (key, value) pairs
        return PebbleTool.traverse(dictionary=self._dictionary)


class PebbleModel:
    """ """

    def __init__(self) -> None:
        """ """

        pass


class Pebble:
    """
    The main class for the Pebble library.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> None:
        """
        Initialize a new Pebble object.

        Args:
            name (str, optional): The name of the database.
            path (Union[Path, str], optional): The path to the database file.

        Returns:
            None
        """

        # Initialize the databases dictionary
        self._databases: dict[str, PebbleDatabase] = {}

        # Initialize the tables dictionary
        self._tables: dict[str, PebbleTable] = {}

    def empty(
        self,
        databass: bool = False,
        tables: bool = False,
    ) -> bool:
        """
        Check if the Pebble object is empty.

        This method will check if the Pebble object is empty, i.e. if the databases and tables dictionaries are empty.

        Args:
            databass (bool, optional): Whether to check the databases.
            tables (bool, optional): Whether to check the tables.

        Returns:
            bool: True if the Pebble object is empty, False otherwise.
        """

        # Initialize the result list
        result: List[bool] = []

        # Check if the databases should be checked
        if databass:
            # Check if the databases dictionary is empty
            result.append(len(self._databases) == 0)

        # Check if the tables should be checked
        if tables:
            # Check if the tables dictionary is empty
            result.append(len(self._tables) == 0)

        # Return True if all the conditions are met
        return all(result)

    def database(
        self,
        name: str,
        path: Optional[Path] = None,
    ) -> PebbleDatabase:
        """
        Get or create a database.

        Args:
            name (str): The name of the database.

        Returns:
            PebbleDatabase: The database.
        """

        # Check if the database exists
        if name in self._databases:
            # Return the database
            return self._databases[name]

        # Create the database builder
        builder: PebbleDatabaseBuilder = PebbleDatabaseBuilder()

        # Set the identifier
        builder.with_identifier()

        # Set the name
        builder.with_name(value=name)

        # Check if the path is None
        if not path:
            # Configure the path
            path = Path(
                ".",
                f"{name}.json",
            )

        # Set the path
        builder.with_path(value=path)

        # Build the database
        database: PebbleDatabase = builder.build()

        # Add the database to the Pebble object
        self._databases[name] = database

        # Return the database
        return database

    def get_or_create_database(
        self,
        name: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> PebbleDatabase:
        """
        Get or create a database.

        Args:
            name (str, optional): The name of the database.
            path (Union[Path, str], optional): The path to the database file.

        Returns:
            PebbleDatabase: The database.
        """

        # Check if the path is a string
        if path is not None or not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path or ".")

        # Check if the database exists
        if name in self._databases:
            # Return the database
            return self._databases[name]

        # Create the database
        database = PebbleDatabase.from_file(path=path)

        # Add the database to the dictionary
        self._databases[name] = database

        # Return the database
        return database

    def get_or_create_table(
        self,
        name: Optional[str] = None,
        path: Optional[Union[Path, str]] = None,
    ) -> PebbleTable:
        """
        Get or create a table.

        Args:
            name (str, optional): The name of the table.
            path (Union[Path, str], optional): The path to the table file.

        Returns:
            PebbleTable: The table.
        """

        # Check if the path is a string
        if path is not None or not isinstance(
            path,
            Path,
        ):
            # Convert the string to a Path object
            path = Path(path or ".")

        # Check if the table exists
        if name in self._tables:
            # Return the table
            return self._tables[name]

        # Create the table
        table = PebbleTable.from_file(path=path)

        # Add the table to the dictionary
        self._tables[name] = table

        # Return the table
        return table

    def table(
        self,
        name: str,
        path: Optional[Path] = None,
    ) -> PebbleTable:
        """
        Get or create a table.

        Args:
            name (str): The name of the table.

        Returns:
            PebbleTable: The table.
        """

        # Check if the table exists
        if name in self._tables:
            # Return the table
            return self._tables[name]

        # Create the table builder
        builder: PebbleTableBuilder = PebbleTableBuilder()

        # Set the identifier
        builder.with_identifier()

        # Set the name
        builder.with_name(value=name)

        # Check if the path is None
        if not path:
            # Configure the path
            path = Path(
                ".",
                f"{name}.json",
            )

        # Set the path
        builder.with_path(value=path)

        # Build the table
        table: PebbleTable = builder.build()

        # Add the table to the dictionary
        self._tables[name] = table

        # Return the table
        return table
