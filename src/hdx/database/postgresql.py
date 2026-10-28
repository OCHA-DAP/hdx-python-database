"""PostgreSQL specific utilities"""

import logging
import subprocess
import time
from os import environ
from pathlib import Path

from .dburi import get_params_from_connection_uri, remove_driver_from_uri

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


class PostgresError(Exception):
    pass


def wait_for_postgresql(db_uri: str, timeout: int | None = None) -> None:
    """Waits for PostgreSQL database to be up

    Args:
        db_uri (str): Connection URI
        timeout (Optional[int]): Seconds to wait. Defaults to None (wait forever).

    Returns:
        None
    """
    connecting_string = "Checking for PostgreSQL..."
    db_uri_nd = remove_driver_from_uri(db_uri)
    start = time.monotonic()
    while True:
        try:
            logger.info(connecting_string)
            connection = psycopg.connect(db_uri_nd, connect_timeout=3)
            connection.close()
            logger.info("PostgreSQL is running!")
            break
        except psycopg.OperationalError:
            if timeout is not None and time.monotonic() - start > timeout:
                raise PostgresError(f"PostgreSQL not available after {timeout}s")
            time.sleep(1)


def restore_from_pgfile(db_uri: str, pg_restore_file: Path | str) -> str:
    """Restore database from a pg_restore file created by pg_backup using the
    pg_restore command.

    Args:
        db_uri (str): Connection URI
        pg_restore_file (Path | str): Path to the pg_restore database file

    Returns:
        str: Output from the pg_restore command
    """
    pg_restore_file = Path(pg_restore_file).absolute()
    db_params = get_params_from_connection_uri(db_uri)
    if pg_restore_file.suffix == ".xz":
        decompress = subprocess.Popen(
            ("unxz", "-c", "-d", pg_restore_file), stdout=subprocess.PIPE
        ).stdout
    else:
        decompress = None
    subprocess_params = ["pg_restore", "-c"]
    for key, value in db_params.items():
        match key:
            case "database":
                subprocess_params.append("-d")
            case "host":
                subprocess_params.append("-h")
            case "port":
                subprocess_params.append("-p")
            case "username":
                subprocess_params.append("-U")
            case _:
                continue
        subprocess_params.append(f"{value}")

    if not decompress:
        subprocess_params.append(str(pg_restore_file))
    env = environ.copy()
    password = db_params.get("password")
    if password:
        env["PGPASSWORD"] = password
    process = subprocess.run(
        subprocess_params,
        env=env,
        capture_output=True,
        encoding="utf-8",
        stdin=decompress,
        check=False,
    )
    try:
        process.check_returncode()
    except subprocess.CalledProcessError as ex:
        command = " ".join(subprocess_params)
        raise PostgresError(
            f"{command} failed. Return code: {process.returncode}. Error: {process.stderr}"
        ) from ex

    for line in process.stdout.splitlines():
        logger.info(line)
    return process.stdout
