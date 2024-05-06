"""Connection URI utilities"""

from typing import Any, Dict, Optional, Union
from urllib.parse import urlsplit


def get_params_from_connection_uri(
    db_uri: str,
    include_driver: bool = True,
) -> Dict[str, Any]:
    """Gets PostgreSQL database connection parameters from connection URI

    Args:
        db_uri (str): Connection URI
        include_driver (bool): Whether to include driver in params. Defaults to True.

    Returns:
        Dict[str,Any]: Dictionary of database connection parameters
    """
    result = urlsplit(db_uri)
    dialectdriver = result.scheme.split("+")
    if len(dialectdriver) == 1:
        dialect = dialectdriver[0]
        driver = None
    else:
        dialect, driver = dialectdriver
    result = {
        "database": result.path[1:],
        "host": result.hostname,
        "port": result.port,
        "username": result.username,
        "password": result.password,
        "dialect": dialect,
    }
    if include_driver:
        result["driver"] = driver
    return result


def get_connection_uri(
    database: Optional[str] = None,
    host: Optional[str] = None,
    port: Union[int, str, None] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    dialect: str = "postgresql",
    driver: Optional[str] = None,
    include_driver: bool = True,
) -> str:
    """Gets connection uri from database connection parameters

    Args:
        database (Optional[str]): Database name
        host (Optional[str]): Host where database is located
        port (Union[int, str, None]): Database port
        username (Optional[str]): Username to log into database
        password (Optional[str]): Password to log into database
        dialect (str): Database dialect. Defaults to "postgresql".
        driver (Optional[str]): Database driver. Defaults to None (psycopg if postgresql or None)
        include_driver (bool): Whether to include driver in uri. Defaults to True.

    Returns:
        str: Connection URI
    """
    if not database:
        return ""
    strings = [dialect]
    if include_driver:
        if dialect == "postgresql" and driver is None:
            driver = "psycopg"
        if driver:
            strings.append(f"+{driver}")
    strings.append("://")
    if username:
        strings.append(username)
        if password:
            strings.append(f":{password}@")
        else:
            strings.append("@")
    if host:
        strings.append(host)
    if port is not None:
        strings.append(f":{port}")
    if database:
        strings.append(f"/{database}")
    return "".join(strings)


def remove_driver_from_uri(db_uri: str) -> str:
    """Remove driver from connection URI

    Args:
        db_uri (str): Connection URI

    Returns:
        str: Connection URI with driver removed
    """
    result = urlsplit(db_uri)
    dialectdriver = result.scheme
    dialect = dialectdriver.split("+")[0]
    return db_uri.replace(dialectdriver, dialect)
