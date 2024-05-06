"""Database utilities"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from sqlalchemy import Engine, TableClause, create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.ddl import CreateSchema, DropSchema
from sqlalchemy.util import Properties

from ._version import version as __version__  # noqa: F401
from .dburi import get_connection_uri
from .no_timezone import Base as NoTZBase
from .postgresql import wait_for_postgresql
from .views import view
from .with_timezone import Base as TZBase

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    pass


def do_nothing_fn():
    return None


class Database:
    """Database helper class to handle ssh tunnels, waiting for PostgreSQL to
    be up etc. Can be used in a with statement returning a Database object that
    if reflect is True will have a variable reflected_classes containing the
    reflected classes. Either an SQLAlchemy engine, database uri or various
    database parameters must be supplied.

    db_has_tz which defaults to False indicates whether database datetime
    columns have timezones. If not, conversion occurs between Python datetimes
    with timezones to timezoneless database columns (but not when using
    reflection). If table_base is supplied, db_has_tz is ignored.

    There is an option to wipe and create an empty schema in the database by
    setting recreate_schema to True and setting a schema_name ("public" is the
    default).

    If a prepare function is supplied in prepare_fn, it will be executed before
    Base.metadata.create_all and the results of it returned in instance variable
    prepare_results.

    Args:
        engine (Optional[Engine]): SQLAlchemy engine to use.
        db_uri (Optional[str]): Connection URI.
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
        recreate_schema (bool): Whether to recreate schema
        schema_name (str): Database schema name. Defaults to "public".
        prepare_fn (Callable[[], None]]): Function to call before Base.metadata.create_all.
        ssh_host (str): SSH host (the server to connect to)
        ssh_port (int): SSH port. Defaults to 22.
        ssh_username (str): SSH username
        ssh_password (str): SSH password
        ssh_private_key: Location of SSH private key (instead of ssh_password)
        For more advanced usage, see SSHTunnelForwarder documentation.

    """

    def __init__(
        self,
        engine: Optional[Engine] = None,
        db_uri: Optional[str] = None,
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
        schema_name = None
        if len(kwargs) == 0:
            recreate_schema = False
            prepare_fn = do_nothing_fn
        else:
            recreate_schema = kwargs.pop("recreate_schema", False)
            schema_name = kwargs.pop("schema", "public")
            prepare_fn = kwargs.pop("prepare_fn", do_nothing_fn)
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
        if not engine and not db_uri:
            db_uri = get_connection_uri(
                database,
                host,
                port,
                username,
                password,
                dialect=dialect,
                driver=driver,
            )
            if not db_uri:
                raise DatabaseError(
                    "No engine, database uri or connection parameters provided!"
                )
        if not engine and dialect == "postgresql":
            wait_for_postgresql(db_uri)
        if not table_base:
            if db_has_tz:
                table_base = TZBase
            else:
                table_base = NoTZBase
        if not engine:
            engine = create_engine(db_uri, poolclass=NullPool, echo=False)
        self.engine: Engine = engine
        if recreate_schema:
            self.recreate_schema(engine, schema_name)
        self.prepare_results = prepare_fn()
        self.session, self.reflected_classes = self.create_session(
            engine,
            table_base=table_base,
            reflect=reflect,
        )

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        self.session.close()
        if self.server is not None:
            self.server.stop()

    def get_engine(self) -> Engine:
        """Returns SQLAlchemy engine.

        Returns:
            sqlalchemy.Engine: SQLAlchemy engine
        """
        return self.engine

    def get_session(self) -> Session:
        """Returns SQLAlchemy session.

        Returns:
            sqlalchemy.orm.Session: SQLAlchemy session
        """
        return self.session

    def get_reflected_classes(self) -> Any:
        """Gets reflected classes.

        Returns:
            Any: Reflected classes
        """
        return self.reflected_classes

    def get_prepare_results(self) -> Any:
        """Returns results from prepare function.

        Returns:
            Any: Results from prepare function
        """
        return self.prepare_results

    @staticmethod
    def create_session(
        engine: Optional[Engine] = None,
        db_uri: Optional[str] = None,
        table_base: Type[DeclarativeBase] = NoTZBase,
        reflect: bool = False,
    ) -> Tuple[Session, Optional[Properties]]:
        """Creates SQLAlchemy session given SQLAlchemy engine or database uri
        (one of which must be supplied). Tables must inherit from Base in
        hdx.utilities.database unless base is defined. If reflect is True,
        classes will be reflected from an existing database and the reflected
        classes will be returned. Note that type annotation maps don't work
        with reflection.

        Args:
            engine (Optional[Engine]): SQLAlchemy engine to use. Defaults to None (create from db_uri).
            db_uri (Optional[str]): Connection URI. Defaults to None (use engine).
            table_base (Type[DeclarativeBase]): Base database table class. Defaults to NoTZBase.
            reflect (bool): Whether to reflect existing tables. Defaults to False.

        Returns:
            Tuple[Session, Optional[Properties]]: (SQLAlchemy session, reflected classes if available)
        """
        if not engine:
            if db_uri is None:
                raise DatabaseError("No engine or database uri provided!")
            engine = create_engine(db_uri, poolclass=NullPool, echo=False)
        Session = sessionmaker(bind=engine)
        if reflect:
            Base = automap_base(declarative_base=table_base)
            Base.prepare(autoload_with=engine)
            reflected_classes = Base.classes
        else:
            table_base.metadata.create_all(engine)
            reflected_classes = None
        session = Session()
        return session, reflected_classes

    @staticmethod
    def recreate_schema(engine: Engine, schema_name: str = "public") -> bool:
        """Wipe and create empty schema in database using SQLAlchemy.

        Args:
            engine (Engine): SQLAlchemy engine to use.
            schema_name (str): Schema name. Defaults to "public".

        Returns:
            bool: True if all successful, False if not
        """
        # Wipe and create an empty schema
        try:
            with engine.connect() as connection:
                connection.execute(
                    DropSchema(schema_name, cascade=True, if_exists=True)
                )
                connection.commit()
                connection.execute(
                    CreateSchema(schema_name, if_not_exists=True)
                )
                connection.commit()
                return True
        except SQLAlchemyError:
            return False

    @staticmethod
    def prepare_view(view_params: Dict) -> TableClause:
        """Prepare SQLAlchemy view from dictionary with keys: name, metadata and
        selectable. Must be run before Base.metadata.create_all.

        Args:
            view_params (Dict): Dictionary with keys name, metadata, selectable

        Returns:
            TableClause: SQLAlchemy View
        """
        return view(**view_params)

    @classmethod
    def prepare_views(cls, view_params_list: List[Dict]) -> List[TableClause]:
        """Prepare SQLAlchemy views from a list of dictionaries with keys: name,
        metadata and selectable. Must be run before Base.metadata.create_all.

        Args:
            view_params_list (List[Dict]): List of dictionaries with view parameters
        Returns:
            List[TableClause]: SQLAlchemy Views
        """
        results = []
        for view_params in view_params_list:
            results.append(cls.prepare_view(view_params))
        return results
