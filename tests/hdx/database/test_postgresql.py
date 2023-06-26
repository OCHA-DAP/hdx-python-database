"""PostgreSQL Utility Tests"""

from . import PsycopgConnection
from hdx.database.postgresql import wait_for_postgresql


class TestPostgreSQL:
    connection_uri_pg = (
        "postgresql+psycopg://myuser:mypass@myserver:1234/mydatabase"
    )

    def test_wait_for_postgresql(self, mock_psycopg):
        PsycopgConnection.connected = False
        wait_for_postgresql(TestPostgreSQL.connection_uri_pg)
        assert PsycopgConnection.connected is True
