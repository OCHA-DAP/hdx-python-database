# -*- coding: utf-8 -*-
"""Postgres specific utilities"""
import logging
import time
from typing import Optional, Union

try:
    import psycopg2
except ImportError:
    # dependency missing, issue a warning
    import warnings
    warnings.warn('psycopg2 not found! Please install hdx-python-database[postgres] to enable.')

logger = logging.getLogger(__name__)


def wait_for_postgres(database, host, port, username, password):
    # type: (Optional[str], Optional[str], Union[int, str, None], Optional[str], Optional[str]) -> None
    """Waits for PostgreSQL database to be up

    Args:
        database (Optional[str]): Database name
        host (Optional[str]): Host where database is located
        port (Union[int, str, None]): Database port
        username (Optional[str]): Username to log into database
        password (Optional[str]): Password to log into database

    Returns:
        None
    """
    connecting_string = 'Checking for PostgreSQL...'
    if port is not None:
        port = int(port)
    while True:
        try:
            logger.info(connecting_string)
            connection = psycopg2.connect(
                database=database,
                host=host,
                port=port,
                user=username,
                password=password,
                connect_timeout=3
            )
            connection.close()
            logger.info('PostgreSQL is running!')
            break
        except psycopg2.OperationalError:
            time.sleep(1)
