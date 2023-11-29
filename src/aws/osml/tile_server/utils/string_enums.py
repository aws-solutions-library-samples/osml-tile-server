#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from enum import Enum


class AutoStringEnum(Enum):
    """
    A class used to represent an Enum where the value of the Enum member is the same as the name of the Enum member.

    Attributes
    ----------
    name : str
        The name of the Enum member as a string.

    Methods
    -------
    _generate_next_value_(name, start, count, last_values)
        Returns the next value of the enumeration which is the same as the name.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Parameters
        ----------
        name : str
            The name of the Enum member.
        start : int
            The initial integer.
        count : int
            The number of existing members.
        last_values : list
            The list of values of existing members.

        Returns
        ----------
        str
            The next value of the enumeration which is the same as the name.
        """
        return name


class AutoLowerStringEnum(Enum):
    """
    A class to represent an Enum where the value of the Enum member is the lowercase version of the name of the Enum member.

    Attributes
    ----------
    name : str
        The name of the Enum member as a string.

    Methods
    -------
    _generate_next_value_(name, start, count, last_values)
        Returns the next value of the enumeration which is the lowercase version of the name.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Parameters
        ----------
        name : str
            The name of the Enum member.
        start : int
            The initial integer.
        count : int
            The number of existing members.
        last_values : list
            The list of values of existing members.

        Returns
        ----------
        str
            The next value of the enumeration which is the lowercase version of the name.
        """
        return name.lower()


class AutoUnderscoreStringEnum(Enum):
    """
    A class used to represent an Enum where the value of the Enum member is the name of the Enum member with
    underscores replaced with spaces.

    Attributes
    ----------
    name : str
        The name of the Enum member as a string.

    Methods ------- _generate_next_value_(name, start, count, last_values) Returns the next value of the enumeration
    which is the name of the Enum member with underscores replaced with spaces.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Parameters
        ----------
        name : str
            The name of the Enum member.
        start : int
            The initial integer.
        count : int
            The number of existing members.
        last_values : list
            The list of values of existing members.

        Returns ---------- str The next value of the enumeration which is the name of the Enum member with
        underscores replaced with spaces.
        """
        return name.replace("_", " ")
