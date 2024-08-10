from __future__ import annotations
import json
import math
import uuid


def json_copy(obj):
    """Creates a deep copy of a JSON-serializable object.

    Args:
        obj: Any JSON-serializable object.

    Returns:
        A deep copy of the input object.
    """
    return json.loads(json.dumps(obj))


def get_id():
    """Generates a unique identifier.

    Returns:
        str: A unique identifier string.
    """
    return f'a{uuid.uuid1()}'.replace('-', '')


def trim(s: str):
    """Removes leading/trailing whitespace and quotes from a string.

    Args:
        s: The input string to trim.

    Returns:
        str: The trimmed string.
    """
    s = s.strip()
    if s[:1] == '"' and s[-1:] == '"':
        return s[1:-1]
    return s


def in_quotes(index, string=None, last=None):
    """Determines if a character in a string is within quotes.

    This function can be used in two ways:
    1. As a generator that yields information about each character in a string.
    2. To check a specific index in a string.

    Args:
        index: Either an integer index or the string to analyze.
        string: The string to analyze (if index is an integer).
        last: The previous quote state (used for continuation).

    Returns:
        If used as a generator, yields tuples (index, char, is_in_quotes).
        If used for a specific index, returns the updated quote state.
    """
    if isinstance(index, str):
        def gen():
            in_string = ['"', False]
            for i in range(len(index)):
                in_string = in_quotes(i, index, in_string)
                yield i, index[i], in_string[1]
        return gen()
    in_string = last
    for quote in ['"""', "'''", '"', "'"]:
        if string[index].startswith(quote):
            if in_string[1] and in_string[0] == quote:
                in_string[1] = False
                break
            elif not in_string[1]:
                in_string = [quote, True]
                break
    return in_string


def extract_json(txt: str):
    """Extracts the first complete JSON object from a string.

    Args:
        txt: The input string containing JSON.

    Returns:
        tuple: A tuple containing:
            - The index after the extracted JSON object.
            - The extracted JSON string.
    """
    s = ''
    i = 0
    if txt.count('{') > 0:
        n, started = 0, False
        for index, c, is_in_quotes in in_quotes(txt):
            if not is_in_quotes:
                if c == '{':
                    started = True
                    n += 1
                if c == '}':
                    n -= 1
            s += c
            i = index + 1
            if n == 0 and started:
                break
    return i, s


def safe_is_nan(v):
    """Checks if a value is NaN (Not a Number).

    This function safely checks whether the input value is NaN,
    handling cases where the input might not support the isnan() function.

    Args:
        v: The value to check. Can be of any type.

    Returns:
        bool: True if the value is NaN, False otherwise or if the check fails.

    Example:
        >>> safe_is_nan(float('nan'))
        True
        >>> safe_is_nan(5)
        False
        >>> safe_is_nan("not a number")
        False
    """
    try:
        return math.isnan(v)
    except:
        return False


def try_number(value, _type=float, on_fail_return_value=None, asignment=None, nan_allowed=False):
    """Attempts to convert a value to a number.

    Args:
        value: The value to convert.
        _type: The desired number type (default is float).
        on_fail_return_value: The value to return if conversion fails.
        asignment: An optional list to store the converted value.
        nan_allowed: Whether NaN values are allowed.

    Returns:
        The converted number, or on_fail_return_value if conversion fails.
    """
    try:
        v = _type(value)
        a = isinstance(asignment, list)
        if a:
            if len(asignment) < 1:
                asignment.append(v)
            else:
                asignment[0] = v
        if not nan_allowed and safe_is_nan(v):
            return on_fail_return_value
        return v if not a else True
    except:
        return on_fail_return_value


def try_json_loads(data: str, func=lambda a: a):
    """Attempts to parse a JSON string.

    Args:
        data: The string to parse as JSON.
        func: A function to apply to the data if parsing fails.

    Returns:
        The parsed JSON object, or the result of func(data) if parsing fails.
    """
    try:
        return json.loads(data)
    except:
        return func(data)


class PipeObject(object):
    """A base class for pipeline objects with JSON serialization support.

    This class provides methods for string representation and JSON conversion.
    """

    def __str__(self):
        """Returns a string representation of the object."""
        def display(value):
            if isinstance(value, str):
                return f"'{value}'"
            return str(value)

        values = [f'{name}={display(value)}'
                  for name, value in self.__dict__.items()]

        return f'{type(self).__name__}({", ".join(values)})'

    def __repr__(self):
        """Returns a string representation of the object."""
        return str(self)

    def to_json(self) -> dict:
        """Converts the object to a JSON-serializable dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        return self.__dict__

    def from_json(self, json: dict):
        """Populates the object's attributes from a JSON dictionary.

        Args:
            json: A dictionary containing attribute values.

        Returns:
            self: The updated object instance.
        """
        self.__dict__ = json
        return self

