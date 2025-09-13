"""
Author: Louis Goodnews
Date: 2025-09-05
"""

from typing import Final, List


__all__: Final[List[str]] = [
    "PebbleField",
    "PebbleFieldFactory",
    "PebbleFieldBuilder",
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
            "date": datetime.date,
            "datetime": datetime,
            "decimal": (Decimal, float, int),
            "dictionary": dict,
            "float": float,
            "integer": int,
            "list": list,
            "path": Path,
            "set": set,
            "string": str,
            "time": datetime.time,
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
            raise PebbleFieldValidationError(f"The field {self._name} must be {self._default}.")
            raise PebbleFieldValidationError(f"The field {self._name} must be {self._default}.")

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


class PebbleFieldBuilder:
    """
    A builder class for creating PebbleField objects.
    """

    def __init__(self) -> None:
        """
        Initialize a new PebbleFieldBuilder object.

        Returns:
            None
        """

        # Store the configuration in an instance variable
        self._configuration: dict[str, Any] = {}

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
        other: "PebbleFieldBuilder",
    ) -> bool:
        """
        Check if the builder is equal to another builder.

        Args:
            other (PebbleFieldBuilder): The other builder to compare to.

        Returns:
            bool: True if the builders are equal, False otherwise.
        """

        # Check if the other object is a PebbleFieldBuilder
        if not isinstance(
            other,
            PebbleFieldBuilder,
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
        return f"PebbleFieldBuilder(configuration={self._configuration})"

    def __str__(self) -> str:
        """
        Get the string representation of the builder.

        Returns:
            str: The string representation of the builder.
        """

        # Return the string representation of the builder
        return str(self._configuration)

    def build(self) -> PebbleField:
        """
        Build the field.

        Returns:
            PebbleField: The field.
        """

        # Create and return the field
        return PebbleFieldFactory.create(**self._configuration)

    def with_choices(
        self,
        value: Iterable[Any],
    ) -> Self:
        """
        Set the choices.

        Args:
            value (Iterable[Any]): The choices to set.

        Returns:
            Self: The builder.
        """

        # Set the choices
        self._configuration["choices"] = value

        # Return the builder
        return self

    def with_default(
        self,
        value: Optional[Any] = None,
    ) -> Self:
        """
        Set the default value.

        Args:
            value (Optional[Any]): The default value to set. Defaults to None.

        Returns:
            Self: The builder.
        """

        # Set the default value
        self._configuration["default"] = value

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

    def with_required(
        self,
        value: bool = False,
    ) -> Self:
        """
        Set the required flag.

        Args:
            value (bool): The required flag to set. Defaults to False.

        Returns:
            Self: The builder.
        """

        # Set the required flag
        self._configuration["required"] = value

        # Return the builder
        return self

    def with_type(
        self,
        value: Literal[
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
    ) -> Self:
        """
        Set the type.

        Args:
            value (Literal[
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
                ]): The type to set.

        Returns:
            Self: The builder.
        """

        # Set the type
        self._configuration["type_"] = value

        # Return the builder
        return self

    def with_validator(
        self,
        value: Optional[Callable[[Any], bool]] = None,
    ) -> Self:
        """
        Set the validator.

        Args:
            value (Optional[Callable[[Any], bool]]): The validator to set. Defaults to None.

        Returns:
            Self: The builder.
        """

        # Set the validator
        self._configuration["validator"] = value

        # Return the builder
        return self
