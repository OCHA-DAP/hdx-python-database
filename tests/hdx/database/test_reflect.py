"""Database Utility Tests"""

from datetime import datetime
from os import remove
from os.path import join
from shutil import copyfile

import pytest
from sqlalchemy import select
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

from hdx.database import Database


class TestReflect:
    dbpath = join("tests", "test_database.db")
    testdb = join("tests", "fixtures", "test.db")

    @pytest.fixture(scope="function")
    def database_to_reflect(self):
        try:
            remove(TestReflect.dbpath)
            copyfile(TestReflect.testdb, TestReflect.dbpath)
        except OSError:
            pass
        return f"sqlite:///{TestReflect.dbpath}"

    def test_get_reflect_session(self, database_to_reflect):
        with Database(
            database=TestReflect.dbpath,
            port=None,
            dialect="sqlite",
            reflect=True,
        ) as dbdatabase:
            dbengine = dbdatabase.get_engine()
            assert str(dbengine.url) == "sqlite:///tests/test_database.db"
            dbsession = dbdatabase.get_session()
            assert isinstance(dbdatabase.base, DeclarativeAttributeIntercept)
            assert str(dbsession.bind.engine.url) == database_to_reflect
            Table1 = dbdatabase.get_reflected_classes().table1
            row = dbsession.execute(select(Table1)).scalar_one()
            assert row.id == "1"
            assert row.col1 == "wfrefds"
            # with reflection, type annotation maps do not work and hence
            # we don't have a timezone here
            assert row.date1 == datetime(1993, 9, 23, 14, 12, 56, 111000)
