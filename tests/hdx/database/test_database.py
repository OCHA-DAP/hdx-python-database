"""Database Utility Tests"""

from datetime import datetime, timezone
from os import remove
from os.path import join

import pytest
from sqlalchemy import select

from .dbtestdate import DBTestDate, date_view_params
from hdx.database import Database, DatabaseError
from hdx.database.no_timezone import Base as NoTZBase


class TestDatabase:
    dbpath = join("tests", "test_database.db")

    @pytest.fixture(scope="function")
    def nodatabase(self):
        try:
            remove(TestDatabase.dbpath)
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
            dbsession = dbdatabase.get_session()
            assert str(dbsession.bind.engine.url) == nodatabase
            assert dbdatabase.base == NoTZBase
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
            dbdatabase.drop_all()

    def test_errors(self):
        with pytest.raises(DatabaseError):
            Database.create_session()

        with pytest.raises(DatabaseError):
            with Database():
                pass
