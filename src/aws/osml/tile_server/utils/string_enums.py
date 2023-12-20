#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from enum import Enum


class AutoStringEnum(Enum):
    """
    A class used to represent an Enum where the value of the Enum member is the same as the name of the Enum member.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Function to iterate through the Enum members.

        :param: name: Name of the Enum member.
        :param: start: Initial integer to start with.
        :param: count: Number of existing members.
        :param: last_values: List of values for existing members.

        :return: The next value of the enumeration which is the same as the name.
        """
        return name


class AutoLowerStringEnum(Enum):
    """
    A class to represent an Enum where the value of the Enum member is the lowercase version of the name associated with
    the Enum member.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Function to iterate through the Enum members.

        :param: name: The name of the Enum member.
        :param: start: The initial integer.
        :param: count: The number of existing members.
        :param: last_values: The list of values associated with existing members.

        :return: The next value of the enumeration which is the lowercase version of the name.
        """

        return name.lower()


class AutoUnderscoreStringEnum(Enum):
    """
    A class used to represent an Enum where the value of the Enum member is the name of the Enum member with
    underscores replaced with spaces.
    """

    @staticmethod
    def _generate_next_value_(name, start, count, last_values) -> str:
        """
        Function to iterate through the Enum members.

        :param: name: Name of the Enum member.
        :param: start: Initial integer to start with.
        :param: count: Number of existing members.
        :param: last_values: List of values of existing members.

        :return: Next value of enumeration which is the name of the Enum member with underscores replaced by spaces.
        """

        return name.replace("_", " ")
