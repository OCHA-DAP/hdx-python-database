"""PostgreSQL Utility Tests"""

import pytest

from . import PsycopgConnection
from hdx.database.postgresql import (
    PostgresError,
    restore_from_pgfile,
    wait_for_postgresql,
)


class TestPostgreSQL:
    connection_uri_pg = (
        "postgresql+psycopg://myuser:mypass@myserver:1234/mydatabase"
    )

    def test_wait_for_postgresql(self, mock_psycopg):
        PsycopgConnection.connected = False
        wait_for_postgresql(TestPostgreSQL.connection_uri_pg)
        assert PsycopgConnection.connected is True

    def test_restore_from_pgfile(self, mock_subprocess, mock_engine):
        db_uri = "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
        pg_restore_file = "test.pg_restore"
        assert restore_from_pgfile(db_uri, pg_restore_file) == "WORKED!"

    def test_restore_from_pgfile_raises(self):
        db_uri = "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
        pg_restore_file = "NOT_EXIST.pg_restore"
        with pytest.raises(PostgresError):
            restore_from_pgfile(db_uri, pg_restore_file)
