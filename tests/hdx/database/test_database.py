"""Database Utility Tests"""
import copy
import os
from collections import namedtuple
from os.path import join

import psycopg
import pytest
from sshtunnel import SSHTunnelForwarder

from hdx.database import Database
from hdx.database.postgresql import remove_driver_from_uri, wait_for_postgresql


class TestDatabase:
    connected = False
    started = False
    stopped = False
    dbpath = join("tests", "test_database.db")
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

    @pytest.fixture(scope="function")
    def nodatabase(self):
        try:
            os.remove(TestDatabase.dbpath)
        except OSError:
            pass
        return f"sqlite:///{TestDatabase.dbpath}"

    @pytest.fixture(scope="function")
    def mock_psycopg(self, monkeypatch):
        def connect(*args, **kwargs):
            if TestDatabase.connected:

                class Connection:
                    @staticmethod
                    def close():
                        return

                return Connection()
            else:
                TestDatabase.connected = True
                raise psycopg.OperationalError

        monkeypatch.setattr(psycopg, "connect", connect)

    @pytest.fixture(scope="function")
    def mock_SSHTunnelForwarder(self, monkeypatch):
        def init(*args, **kwargs):
            return None

        def start(_):
            TestDatabase.started = True

        def stop(_):
            TestDatabase.stopped = True

        monkeypatch.setattr(SSHTunnelForwarder, "__init__", init)
        monkeypatch.setattr(SSHTunnelForwarder, "start", start)
        monkeypatch.setattr(SSHTunnelForwarder, "stop", stop)
        monkeypatch.setattr(SSHTunnelForwarder, "local_bind_host", "0.0.0.0")
        monkeypatch.setattr(SSHTunnelForwarder, "local_bind_port", 12345)

        def get_session(_, db_url):
            class Session:
                bind = namedtuple("Bind", "engine")

                def close(self):
                    return None

            Session.bind.engine = namedtuple("Engine", "url")
            Session.bind.engine.url = db_url
            return Session()

        monkeypatch.setattr(Database, "get_session", get_session)

    def test_get_params_from_connection_uri(self):
        result = Database.get_params_from_connection_uri(
            TestDatabase.connection_uri_pg
        )
        assert result == TestDatabase.params_pg
        result = Database.get_params_from_connection_uri(
            TestDatabase.connection_uri_pg,
            include_driver=False,
        )
        assert result == TestDatabase.params_pg_no_driver
        result = Database.get_params_from_connection_uri(
            TestDatabase.connection_uri_pg_no_driver
        )
        assert result == TestDatabase.params_pg_driver_none
        result = Database.get_params_from_connection_uri(
            TestDatabase.connection_uri_sq
        )
        assert result == TestDatabase.params_sq_driver_none

    def test_get_connection_uri(self):
        result = Database.get_connection_uri(**TestDatabase.params_pg)
        assert result == TestDatabase.connection_uri_pg
        result = Database.get_connection_uri(
            **TestDatabase.params_pg, include_driver=False
        )
        assert result == TestDatabase.connection_uri_pg_no_driver
        result = Database.get_connection_uri(
            **TestDatabase.params_pg_no_driver
        )
        assert result == TestDatabase.connection_uri_pg
        result = Database.get_connection_uri(
            **TestDatabase.params_pg_driver_none
        )
        assert result == TestDatabase.connection_uri_pg
        result = Database.get_connection_uri(
            **TestDatabase.params_sq_no_driver
        )
        assert result == TestDatabase.connection_uri_sq
        result = Database.get_connection_uri(
            **TestDatabase.params_sq_driver_none
        )
        assert result == TestDatabase.connection_uri_sq

    def test_remove_driver_from_uri(self):
        db_uri_nd = remove_driver_from_uri(TestDatabase.connection_uri_pg)
        assert db_uri_nd == TestDatabase.connection_uri_pg_no_driver

    def test_wait_for_postgresql(self, mock_psycopg):
        TestDatabase.connected = False
        wait_for_postgresql(TestDatabase.connection_uri_pg)
        assert TestDatabase.connected is True

    def test_get_session(self, nodatabase):
        with Database(
            database=TestDatabase.dbpath, port=None, dialect="sqlite"
        ) as dbsession:
            assert str(dbsession.bind.engine.url) == nodatabase

    def test_get_session_ssh(self, mock_psycopg, mock_SSHTunnelForwarder):
        with Database(
            ssh_host="mysshhost", **TestDatabase.params_pg
        ) as dbsession:
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
            )
        params = copy.deepcopy(TestDatabase.params_pg)
        del params["password"]
        with Database(
            ssh_host="mysshhost", ssh_port=25, **params
        ) as dbsession:
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
