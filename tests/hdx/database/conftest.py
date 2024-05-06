import psycopg
import pytest

from . import PsycopgConnection


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
