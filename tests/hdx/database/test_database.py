"""Database Utility Tests"""
from collections import namedtuple
from copy import deepcopy
from datetime import datetime, timezone
from os import remove
from os.path import join
from shutil import copyfile

import pytest
from sqlalchemy import select
from sshtunnel import SSHTunnelForwarder

from .dbtestdate import DBTestDate, date_view
from hdx.database import Database
from hdx.database.no_timezone import Base as NoTZBase
from hdx.database.with_timezone import Base as TZBase


class TestDatabase:
    started = False
    stopped = False
    table_base = NoTZBase
    dbpath = join("tests", "test_database.db")
    testdb = join("tests", "fixtures", "test.db")
    params_pg = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "postgresql",
        "driver": "psycopg",
    }

    @pytest.fixture(scope="function")
    def nodatabase(self):
        try:
            remove(TestDatabase.dbpath)
        except OSError:
            pass
        return f"sqlite:///{TestDatabase.dbpath}"

    @pytest.fixture(scope="function")
    def database_to_reflect(self):
        try:
            remove(TestDatabase.dbpath)
            copyfile(TestDatabase.testdb, TestDatabase.dbpath)
        except OSError:
            pass
        return f"sqlite:///{TestDatabase.dbpath}"

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

        def get_session(_, db_url, table_base, reflect):
            class Session:
                bind = namedtuple("Bind", "engine")

                def close(self):
                    return None

            Session.bind.engine = namedtuple("Engine", "url")
            Session.bind.engine.url = db_url
            TestDatabase.table_base = table_base
            return Session()

        monkeypatch.setattr(Database, "get_session", get_session)

    def test_get_session(self, nodatabase):
        assert DBTestDate.__tablename__ == "db_test_date"
        with Database(
            database=TestDatabase.dbpath, port=None, dialect="sqlite"
        ) as dbsession:
            assert str(dbsession.bind.engine.url) == nodatabase
            assert TestDatabase.table_base == NoTZBase
            now = datetime(2022, 10, 20, 22, 35, 55, tzinfo=timezone.utc)
            input_dbtestdate = DBTestDate()
            input_dbtestdate.test_date = now
            dbsession.add(input_dbtestdate)
            dbsession.commit()
            dbtestdate = dbsession.execute(select(DBTestDate)).scalar_one()
            assert dbtestdate.test_date == now
            dbtestdate = dbsession.execute(select(date_view)).scalar_one()
            assert dbtestdate == now

    def test_get_reflect_session(self, database_to_reflect):
        with Database(
            database=TestDatabase.dbpath,
            port=None,
            dialect="sqlite",
            reflect=True,
        ) as dbsession:
            assert TestDatabase.table_base == NoTZBase
            assert str(dbsession.bind.engine.url) == database_to_reflect
            Table1 = dbsession.reflected_classes.table1
            row = dbsession.execute(select(Table1)).scalar_one()
            assert row.id == "1"
            assert row.col1 == "wfrefds"
            # with reflection, type annotation maps do not work and hence
            # we don't have a timezone here
            assert row.date1 == datetime(1993, 9, 23, 14, 12, 56, 111000)

    def test_get_session_ssh(self, mock_psycopg, mock_SSHTunnelForwarder):
        with Database(
            ssh_host="mysshhost", **TestDatabase.params_pg
        ) as dbsession:
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
            )
        params = deepcopy(TestDatabase.params_pg)
        del params["password"]
        with Database(
            ssh_host="mysshhost", ssh_port=25, **params
        ) as dbsession:
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
        assert TestDatabase.table_base == NoTZBase
        with Database(
            ssh_host="mysshhost", ssh_port=25, db_has_tz=True, **params
        ) as dbsession:
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert TestDatabase.table_base == TZBase
        with Database(
            ssh_host="mysshhost", ssh_port=25, table_base=TZBase, **params
        ) as dbsession:
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert TestDatabase.table_base == TZBase
