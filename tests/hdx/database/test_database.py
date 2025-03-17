"""Database Utility Tests"""

from datetime import datetime, timezone
from os import remove
from os.path import exists, join
from tempfile import gettempdir

import pytest
from sqlalchemy import select

from .dbtestdate import DBTestDate, date_view_params
from hdx.database import Database, DatabaseError
from hdx.database.no_timezone import Base as NoTZBase


class TestDatabase:
    def test_get_session(self):
        assert DBTestDate.__tablename__ == "db_test_date"

        def prepare_fn():
            return Database.prepare_views([date_view_params])

        dbpath = join(gettempdir(), "test_database.db")
        if exists(dbpath):
            remove(dbpath)
        with Database(
            database=dbpath,
            port=None,
            dialect="sqlite",
            prepare_fn=prepare_fn,
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert str(dbsession.bind.engine.url) == f"sqlite:///{dbpath}"
            assert dbdatabase._base == NoTZBase
            now = datetime(2022, 10, 20, 22, 35, 55, tzinfo=timezone.utc)
            input_dbtestdate = DBTestDate(test_date=now)
            dbsession.add(input_dbtestdate)
            dbsession.commit()
            dbtestdate = dbsession.execute(select(DBTestDate)).scalar_one()
            assert dbtestdate.test_date == now
            date_view = dbdatabase.get_prepare_results()[0]
            dbtestdate = dbsession.execute(select(date_view)).scalar_one()
            assert dbtestdate == now

            now = datetime(2023, 10, 20, 22, 35, 55, tzinfo=timezone.utc)
            rows = [{"test_date": now}]
            dbdatabase.batch_populate(rows, DBTestDate)
            dbtestdate = (
                dbsession.execute(select(DBTestDate)).all()[1][0].test_date
            )
            assert dbtestdate == now

        remove(dbpath)

    def test_errors(self):
        with pytest.raises(DatabaseError):
            Database.create_session()

        with pytest.raises(DatabaseError):
            with Database():
                pass
