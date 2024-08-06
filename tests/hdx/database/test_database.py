"""Database Utility Tests"""

from copy import deepcopy
from datetime import datetime, timezone
from os import remove
from os.path import join
from shutil import copyfile

import pytest
from sqlalchemy import select

from .dbtestdate import DBTestDate, date_view_params
from hdx.database import Database, DatabaseError
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

    def test_get_session(self, nodatabase):
        assert DBTestDate.__tablename__ == "db_test_date"

        def prepare_fn():
            return Database.prepare_views([date_view_params])

        with Database(
            database=TestDatabase.dbpath,
            port=None,
            dialect="sqlite",
            prepare_fn=prepare_fn,
        ) as dbdatabase:
            dbsession = dbdatabase.session
            assert str(dbsession.bind.engine.url) == nodatabase
            assert TestDatabase.table_base == NoTZBase
            now = datetime(2022, 10, 20, 22, 35, 55, tzinfo=timezone.utc)
            input_dbtestdate = DBTestDate()
            input_dbtestdate.test_date = now
            dbsession.add(input_dbtestdate)
            dbsession.commit()
            dbtestdate = dbsession.execute(select(DBTestDate)).scalar_one()
            assert dbtestdate.test_date == now
            date_view = dbdatabase.get_prepare_results()[0]
            dbtestdate = dbsession.execute(select(date_view)).scalar_one()
            assert dbtestdate == now
        with pytest.raises(DatabaseError):
            Database.create_session()
        with pytest.raises(DatabaseError):
            with Database():
                pass

    def test_get_reflect_session(self, database_to_reflect):
        with Database(
            database=TestDatabase.dbpath,
            port=None,
            dialect="sqlite",
            reflect=True,
        ) as dbdatabase:
            dbengine = dbdatabase.get_engine()
            assert str(dbengine.url) == "sqlite:///tests/test_database.db"
            dbsession = dbdatabase.get_session()
            assert TestDatabase.table_base == NoTZBase
            assert str(dbsession.bind.engine.url) == database_to_reflect
            Table1 = dbdatabase.get_reflected_classes().table1
            row = dbsession.execute(select(Table1)).scalar_one()
            assert row.id == "1"
            assert row.col1 == "wfrefds"
            # with reflection, type annotation maps do not work and hence
            # we don't have a timezone here
            assert row.date1 == datetime(1993, 9, 23, 14, 12, 56, 111000)

    def test_get_session_ssh(
        self,
        mock_psycopg,
        mock_SSHTunnelForwarder,
    ):
        params = deepcopy(TestDatabase.params_pg)
        del params["password"]
        with Database(
            ssh_host="mysshhost", ssh_port=25, db_has_tz=True, **params
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert TestDatabase.table_base == TZBase
        with Database(
            ssh_host="mysshhost", ssh_port=25, table_base=TZBase, **params
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert TestDatabase.table_base == TZBase
        with Database(
            ssh_host="mysshhost", **TestDatabase.params_pg
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser:***@0.0.0.0:12345/mydatabase"
            )
        with Database(
            ssh_host="mysshhost", ssh_port=25, **params
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert TestDatabase.table_base == NoTZBase

    def test_recreate_schema(self, mock_engine):
        db_uri = "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
        assert Database.recreate_schema(mock_engine, db_uri) is True
        db_uri = "Error"
        assert Database.recreate_schema(mock_engine, db_uri) is False

    def test_restore_from_pgfile(self, mock_subprocess, mock_engine):
        db_uri = "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
        pg_restore_file = "test.pg_restore"
        assert (
            Database.restore_from_pgfile(db_uri, pg_restore_file) == "WORKED!"
        )

    def test_restore_from_pgfile_raises(self):
        db_uri = "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
        pg_restore_file = "NOT_EXIST.pg_restore"
        with pytest.raises(DatabaseError):
            Database.restore_from_pgfile(db_uri, pg_restore_file)
