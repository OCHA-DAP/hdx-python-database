"""Other utilities"""

import re


def camel_to_snake_case(string: str) -> str:
    """Convert a ``CamelCase`` name to ``snake_case``.
    (Taken from flask-sqlalchemy code.)

    Args:
        string (str): String to convert

    Returns:
        str: String in snake case
    """
    string = re.sub(
        r"((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))", r"_\1", string
    )
    return string.lower().lstrip("_")
