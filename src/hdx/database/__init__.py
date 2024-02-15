"""Database utilities"""
import logging
from typing import Any, Optional, Type, Union

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import NullPool

from ._version import version as __version__  # noqa: F401
from .dburi import get_connection_uri
from .no_timezone import Base as NoTZBase
from .postgresql import wait_for_postgresql
from .with_timezone import Base as TZBase

logger = logging.getLogger(__name__)


class Database:
    """Database helper class to handle ssh tunnels, waiting for PostgreSQL to
    be up etc. Can be used in a with statement returning a Session object that
    if reflect is True will have a variable reflected_classes containing the
    reflected classes. db_has_tz which defaults to False indicates whether
    database datetime columns have timezones. If not, conversion occurs between
    Python datetimes with timezones to timezoneless database columns (but not
    when using reflection). If table_base is supplied, db_has_tz is ignored.

    Args:
        database (Optional[str]): Database name
        host (Optional[str]): Host where database is located
        port (Union[int, str, None]): Database port
        username (Optional[str]): Username to log into database
        password (Optional[str]): Password to log into database
        dialect (str): Database dialect. Defaults to "postgresql".
        driver (Optional[str]): Database driver. Defaults to None (psycopg if postgresql or None)
        db_has_tz (bool): True if db datetime columns have timezone. Defaults to False.
        table_base (Optional[Type[DeclarativeBase]]): Override table base. Defaults to None.
        reflect (bool): Whether to reflect existing tables. Defaults to False.
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
        dialect: str = "postgresql",
        driver: Optional[str] = None,
        db_has_tz: bool = False,
        table_base: Optional[Type[DeclarativeBase]] = None,
        reflect: bool = False,
        **kwargs: Any,
    ) -> None:
        if port is not None:
            port = int(port)
        if len(kwargs) != 0:
            try:
                import sshtunnel
            except ImportError:
                # dependency missing, log an error
                logger.error(
                    "sshtunnel not found! Please install hdx-python-database[sshtunnel] to enable."
                )
                raise
            ssh_host = kwargs["ssh_host"]
            del kwargs["ssh_host"]
            ssh_port = kwargs.get("ssh_port")
            if ssh_port is not None:
                ssh_port = int(ssh_port)
                del kwargs["ssh_port"]
            else:
                ssh_port = 22
            self.server = sshtunnel.SSHTunnelForwarder(
                (ssh_host, ssh_port),
                remote_bind_address=(host, port),
                **kwargs,
            )
            self.server.start()
            host = self.server.local_bind_host
            port = self.server.local_bind_port
        else:
            self.server = None
        db_uri = get_connection_uri(
            database,
            host,
            port,
            username,
            password,
            dialect=dialect,
            driver=driver,
        )
        if dialect == "postgresql":
            wait_for_postgresql(db_uri)
        if not table_base:
            if db_has_tz:
                table_base = TZBase
            else:
                table_base = NoTZBase
        self.session = self.get_session(
            db_uri, table_base=table_base, reflect=reflect
        )

    def __enter__(self) -> Session:
        return self.session

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.session.close()
        if self.server is not None:
            self.server.stop()

    @staticmethod
    def get_session(
        db_uri: str,
        table_base: Type[DeclarativeBase] = NoTZBase,
        reflect: bool = False,
    ) -> Session:
        """Gets SQLAlchemy session given url. Tables must inherit from Base in
        hdx.utilities.database unless base is defined. If reflect is True,
        classes will be reflected from an existing database and the reflected
        classes are returned in a variable reflected_classes in the returned
        Session object. Note that type annotation maps don't work with
        reflection.

        Args:
            db_uri (str): Connection URI
            table_base (Type[DeclarativeBase]): Base database table class. Defaults to NoTZBase.
            reflect (bool): Whether to reflect existing tables. Defaults to False.

        Returns:
            sqlalchemy.orm.Session: SQLAlchemy session
        """
        engine = create_engine(db_uri, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        if reflect:
            Base = automap_base(declarative_base=table_base)
            Base.prepare(autoload_with=engine)
            classes = Base.classes
        else:
            table_base.metadata.create_all(engine)
            classes = None
        session = Session()
        session.reflected_classes = classes
        return session
