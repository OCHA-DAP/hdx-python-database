"""Database Utility Tests"""

from datetime import datetime
from os import remove
from os.path import join
from shutil import copyfile
from tempfile import gettempdir

from sqlalchemy import select
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

from hdx.database import Database


class TestReflect:
    def test_get_reflect_session(self):
        dbpath = join(gettempdir(), "test_reflect.db")
        testdb = join("tests", "fixtures", "test.db")
        copyfile(testdb, dbpath)
        with Database(
            database=dbpath,
            port=None,
            dialect="sqlite",
            reflect=True,
        ) as dbdatabase:
            dbengine = dbdatabase.get_engine()
            dburi = f"sqlite:///{dbpath}"
            assert str(dbengine.url) == dburi
            dbsession = dbdatabase.get_session()
            assert isinstance(dbdatabase._base, DeclarativeAttributeIntercept)
            assert str(dbsession.bind.engine.url) == dburi
            Table1 = dbdatabase.get_reflected_classes().table1
            row = dbsession.execute(select(Table1)).scalar_one()
            assert row.id == "1"
            assert row.col1 == "wfrefds"
            # with reflection, type annotation maps do not work and hence
            # we don't have a timezone here
            assert row.date1 == datetime(1993, 9, 23, 14, 12, 56, 111000)
        remove(dbpath)
