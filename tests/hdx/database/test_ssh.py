"""Database Utility Tests"""

from copy import deepcopy

from hdx.database import Database
from hdx.database.no_timezone import Base as NoTZBase
from hdx.database.with_timezone import Base as TZBase


class TestSSH:
    params_pg = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "postgresql",
        "driver": "psycopg",
    }

    def test_get_session_ssh(
        self,
        mock_psycopg,
        mock_SSHTunnelForwarder,
    ):
        params = deepcopy(self.params_pg)
        del params["password"]
        with Database(
            ssh_host="mysshhost", ssh_port=25, db_has_tz=True, **params
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert dbdatabase._base == TZBase

        with Database(
            ssh_host="mysshhost", ssh_port=25, table_base=TZBase, **params
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert dbdatabase._base == TZBase

        with Database(ssh_host="mysshhost", **self.params_pg) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser:***@0.0.0.0:12345/mydatabase"
            )
            assert dbdatabase._base == NoTZBase

        with Database(
            ssh_host="mysshhost", ssh_port=25, **params
        ) as dbdatabase:
            dbsession = dbdatabase.get_session()
            assert (
                str(dbsession.bind.engine.url)
                == "postgresql+psycopg://myuser@0.0.0.0:12345/mydatabase"
            )
            assert dbdatabase._base == NoTZBase
