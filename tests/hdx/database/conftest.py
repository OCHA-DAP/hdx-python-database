from collections import namedtuple

import psycopg
import pytest
from sqlalchemy.exc import SQLAlchemyError
from sshtunnel import SSHTunnelForwarder

from . import PsycopgConnection
from .test_database import TestDatabase
from hdx.database import Database


@pytest.fixture(scope="function")
def mock_psycopg(monkeypatch):
    def connect(*args, **kwargs):
        if PsycopgConnection.connected:

            class PGConnection:
                @staticmethod
                def close():
                    return

            return PGConnection()
        else:
            PsycopgConnection.connected = True
            raise psycopg.OperationalError

    monkeypatch.setattr(psycopg, "connect", connect)


@pytest.fixture(scope="function")
def mock_SSHTunnelForwarder(monkeypatch):
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

    def create_session(_, engine, table_base, reflect):
        class Session:
            bind = namedtuple("Bind", "engine")

            def close(self):
                return None

        Session.bind.engine = engine
        TestDatabase.table_base = table_base
        return Session(), None

    monkeypatch.setattr(Database, "create_session", create_session)


@pytest.fixture(scope="function")
def mock_engine():
    class SQAConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback) -> None:
            pass

        @staticmethod
        def execute(statement):
            if "mydatabase" in str(statement):
                return
            raise SQLAlchemyError("Fail!")

        @staticmethod
        def commit():
            return

    class MockEngine:
        @staticmethod
        def connect():
            return SQAConnection()

    return MockEngine()
