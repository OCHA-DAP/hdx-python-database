"""Database Connection Tests"""

from hdx.database.dburi import (
    get_connection_uri,
    get_params_from_connection_uri,
)
from hdx.database.postgresql import remove_driver_from_uri


class TestDBURI:
    params_pg = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "postgresql",
        "driver": "psycopg",
    }
    connection_uri_pg = (
        "postgresql+psycopg://myuser:mypass@myserver:1234/mydatabase"
    )
    params_pg_no_driver = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "postgresql",
    }
    params_pg_driver_none = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "postgresql",
        "driver": None,
    }
    connection_uri_pg_no_driver = (
        "postgresql://myuser:mypass@myserver:1234/mydatabase"
    )
    params_sq_no_driver = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "sqlite",
    }
    params_sq_driver_none = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "sqlite",
        "driver": None,
    }
    connection_uri_sq = "sqlite://myuser:mypass@myserver:1234/mydatabase"

    def test_get_params_from_connection_uri(self):
        result = get_params_from_connection_uri(TestDBURI.connection_uri_pg)
        assert result == TestDBURI.params_pg
        result = get_params_from_connection_uri(
            TestDBURI.connection_uri_pg,
            include_driver=False,
        )
        assert result == TestDBURI.params_pg_no_driver
        result = get_params_from_connection_uri(
            TestDBURI.connection_uri_pg_no_driver
        )
        assert result == TestDBURI.params_pg_driver_none
        result = get_params_from_connection_uri(TestDBURI.connection_uri_sq)
        assert result == TestDBURI.params_sq_driver_none

    def test_get_connection_uri(self):
        result = get_connection_uri(**TestDBURI.params_pg)
        assert result == TestDBURI.connection_uri_pg
        result = get_connection_uri(
            **TestDBURI.params_pg, include_driver=False
        )
        assert result == TestDBURI.connection_uri_pg_no_driver
        result = get_connection_uri(**TestDBURI.params_pg_no_driver)
        assert result == TestDBURI.connection_uri_pg
        result = get_connection_uri(**TestDBURI.params_pg_driver_none)
        assert result == TestDBURI.connection_uri_pg
        result = get_connection_uri(**TestDBURI.params_sq_no_driver)
        assert result == TestDBURI.connection_uri_sq
        result = get_connection_uri(**TestDBURI.params_sq_driver_none)
        assert result == TestDBURI.connection_uri_sq

    def test_remove_driver_from_uri(self):
        db_uri_nd = remove_driver_from_uri(TestDBURI.connection_uri_pg)
        assert db_uri_nd == TestDBURI.connection_uri_pg_no_driver
