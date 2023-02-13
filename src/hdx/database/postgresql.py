"""PostgreSQL specific utilities"""
import logging
import time
from urllib.parse import urlsplit

try:
    import psycopg
except ImportError:
    psycopg = None
    # dependency missing, issue a warning
    import warnings

    warnings.warn(
        "psycopg not found! Please install hdx-python-database[postgresql] to enable."
    )

logger = logging.getLogger(__name__)


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


def wait_for_postgresql(db_uri: str) -> None:
    """Waits for PostgreSQL database to be up

    Args:
        db_uri (str): Connection URI

    Returns:
        None
    """
    connecting_string = "Checking for PostgreSQL..."
    db_uri_nd = remove_driver_from_uri(db_uri)
    while True:
        try:
            logger.info(connecting_string)
            connection = psycopg.connect(db_uri_nd, connect_timeout=3)
            connection.close()
            logger.info("PostgreSQL is running!")
            break
        except psycopg.OperationalError:
            time.sleep(1)
