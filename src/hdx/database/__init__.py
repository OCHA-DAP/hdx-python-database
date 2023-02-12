from ._version import version as __version__  # noqa: F401

"""Database utilities"""
import logging
from typing import Any, Dict, Optional, Union
from urllib.parse import urlsplit

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool
from sshtunnel import SSHTunnelForwarder

from hdx.database.postgresql import wait_for_postgresql

logger = logging.getLogger(__name__)


class HDXBase:
    @declared_attr.directive
    def __tablename__(cls):
        return f"{cls.__name__.lower()}s"


Base = declarative_base(cls=HDXBase)


class Database:
    """Database helper class to handle ssh tunnels, waiting for PostgreSQL to be up etc.

    Args:
        database (Optional[str]): Database name
        host (Optional[str]): Host where database is located
        port (Union[int, str, None]): Database port
        username (Optional[str]): Username to log into database
        password (Optional[str]): Password to log into database
        driver (str): Database driver. Defaults to "postgresql".
        **kwargs: See below
        ssh_host (str): SSH host (the server to connect to)
        ssh_port (int): SSH port. Defaults to 22.
        ssh_username (str): SSH username
        ssh_password (str): SSH password
        ssh_private_key: Location of SSH private key (instead of ssh_password)
        For more advanced usage, see SSHTunnelForwarder documentation.

    """

    def __init__(
        self,
        database: Optional[str] = None,
        host: Optional[str] = None,
        port: Union[int, str, None] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "postgresql",
        **kwargs: Any,
    ) -> None:
        if port is not None:
            port = int(port)
        if len(kwargs) != 0:
            ssh_host = kwargs["ssh_host"]
            del kwargs["ssh_host"]
            ssh_port = kwargs.get("ssh_port")
            if ssh_port is not None:
                ssh_port = int(ssh_port)
                del kwargs["ssh_port"]
            else:
                ssh_port = 22
            self.server = SSHTunnelForwarder(
                (ssh_host, ssh_port),
                remote_bind_address=(host, port),
                **kwargs,
            )
            self.server.start()
            host = self.server.local_bind_host
            port = self.server.local_bind_port
        else:
            self.server = None
        db_url = self.get_connection_uri(
            database, host, port, username, password, driver=driver
        )
        if driver == "postgresql":
            wait_for_postgresql(db_url)
        self.session = self.get_session(db_url)

    def __enter__(self) -> Session:
        return self.session

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.session.close()
        if self.server is not None:
            self.server.stop()

    @staticmethod
    def get_session(db_uri: str) -> Session:
        """Gets SQLAlchemy session given url. Your tables must inherit
        from Base in hdx.utilities.database.

        Args:
            db_uri (str): Connection URI

        Returns:
            sqlalchemy.orm.Session: SQLAlchemy session
        """
        engine = create_engine(db_uri, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        return Session()

    @staticmethod
    def get_params_from_connection_uri(db_uri: str) -> Dict[str, Any]:
        """Gets PostgreSQL database connection parameters from connection URI

        Args:
            db_uri (str): Connection URI

        Returns:
            Dict[str,Any]: Dictionary of database connection parameters
        """
        result = urlsplit(db_uri)
        return {
            "database": result.path[1:],
            "host": result.hostname,
            "port": result.port,
            "username": result.username,
            "password": result.password,
            "driver": result.scheme,
        }

    @staticmethod
    def get_connection_uri(
        database: Optional[str] = None,
        host: Optional[str] = None,
        port: Union[int, str, None] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        driver: str = "postgresql",
    ) -> str:
        """Gets connection uri from database connection parameters

        Args:
            database (Optional[str]): Database name
            host (Optional[str]): Host where database is located
            port (Union[int, str, None]): Database port
            username (Optional[str]): Username to log into database
            password (Optional[str]): Password to log into database
            driver (str): Database driver. Defaults to "postgresql".

        Returns:
            str: Connection URI
        """
        strings = [f"{driver}://"]
        if username:
            strings.append(username)
            if password:
                strings.append(f":{password}@")
            else:
                strings.append("@")
        if host:
            strings.append(host)
        if port is not None:
            strings.append(f":{port}")
        if database:
            strings.append(f"/{database}")
        return "".join(strings)
