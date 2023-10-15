# Summary

The HDX Python Database Library provides utilities for connecting to databases in a standard way including
through an ssh tunnel if needed. It is built on top of SQLAlchemy and simplifies its setup.

# Information

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have
humanitarian related data, please upload your datasets to HDX.

The code for the library is [here](https://github.com/OCHA-DAP/hdx-python-database).
The library has detailed API documentation which can be found in the menu at the top.

Connecting to databases through an ssh tunnel is only available if this library
is installed with extra `sshtunnel`:

    pip install hdx-python-database[sshtunnel]

The `wait_for_postgresql` function is only available if this library is
installed with extra `postgresql`:

    pip install hdx-python-database[postgresql]

## Breaking changes
From 1.2.8, the sshtunnel dependency is optional.

From 1.2.7, default table names are no longer plural. The camel case class name
is converted to snake case, for example `MyTable` becomes `my_table`.

From 1.2.3, Base must be chosen from `hdx.database.no_timezone`
(`db_has_tz=False`: the default) or `hdx.database.with_timezone`
(`db_has_tz=True`).

From 1.2.2, database datetime columns are assumed to be timezoneless unless
db_has_tz is set to True.

From 1.2.1, wait_for_postgresql takes connection URI not database parameters,
get_params_from_sqlalchemy_url renamed to get_params_from_connection_uri
and moved to dburi module, get_sqlalchemy_url renamed to get_connection_uri and
moved to dburi module. New function remove_driver_from_uri in dburi module.
Parameter driver replaced by dialect+driver. Supports Python 3.8 and later.

From 1.1.2, the postgres module is renamed postgresql and function wait_for_postgres
is renamed wait_for_postgresql.

Versions from 1.0.8 support Python 3.6 and later.

Versions from 1.0.6 no longer support Python 2.7.

# Description of Utilities

## Database

Your SQLAlchemy database tables must inherit from `Base` in
`hdx.database.no_timezone` or `hdx.database.with_timezone` eg. :

    from hdx.database.no_timezone import Base
    class MyTable(Base):
        my_col: Mapped[int] = mapped_column(Integer, ForeignKey(MyTable2.col2), primary_key=True)

A default table name is set which can be overridden: it is the camel case class
name to converted to snake case, for example `MyTable` becomes `my_table`.

Then a connection can be made to a database as follows including through an SSH
tunnel (which requires installing `hdx-python-database[sshtunnel]`):

    # Get SQLAlchemy session object given database parameters and
    # if needed SSH parameters. If database is PostgreSQL, will poll
    # till it is up.
    from hdx.database import Database
    with Database(database="db", host="1.2.3.4", username="user",
                  password="pass", dialect="dialect", driver="driver",
                  ssh_host="5.6.7.8", ssh_port=2222, ssh_username="sshuser",
                  ssh_private_key="path_to_key", db_has_tz=True,
                  reflect=False) as session:
        session.query(...)

`db_has_tz` which defaults to `False` indicates whether database datetime
columns have timezones. If `db_has_tz` is `True`, use `Base` from
`hdx.database.with_timezone`, otherwise use `Base` from
`hdx.database.no_timezone`. If `db_has_tz` is `False`, conversion occurs
between Python datetimes with timezones to timezoneless database columns.

If `reflect` (which defaults to `False`) is `True`, classes will be reflected
from an existing database and the reflected classes are returned in a variable
`reflected_classes` in the returned Session object. Note that type annotation
maps don't work with reflection and hence `db_has_tz` will be ignored ie.
there will be no conversion between Python datetimes with timezones to
timezoneless database columns.

## Connection URI

There are functions to handle converting from connection URIs to parameters and
vice-versa as well as a function to remove the driver string from a connection
URI that contains both dialect and driver.

    # Extract dictionary of parameters from database connection URI
    result = get_params_from_connection_uri(
        "postgresql+psycopg://myuser:mypass@myserver:1234/mydatabase"
    )

    # Build database connection URI from dictionary of parameters
    params_pg = {
        "database": "mydatabase",
        "host": "myserver",
        "port": 1234,
        "username": "myuser",
        "password": "mypass",
        "dialect": "postgresql",
        "driver": "psycopg",
    }
    result = get_connection_uri(**params_pg)

    db_uri_nd = remove_driver_from_uri(
        "postgresql+psycopg://myuser:mypass@myserver:1234/mydatabase"
    )

## Views

The method to make views described [here](https://github.com/sqlalchemy/sqlalchemy/wiki/Views#sqlalchemy-14-20-version)
is available in this library.

This allows creating views like this:
```
class DBOrgType(Base):
    __tablename__ = "org_type"

    code: Mapped[str] = mapped_column(String(32), primary_key=True)
    description: Mapped[str] = mapped_column(String(512), nullable=False)


org_type_view = view(
    name="org_type_view",
    metadata=Base.metadata,
    selectable=select(*DBOrgType.__table__.columns),
)

class DBTestDate(Base):
    test_date: Mapped[datetime] = mapped_column(primary_key=True)

date_view_params = {
    "name": "date_view",
    "metadata": Base.metadata,
    "selectable": select(*DBTestDate.__table__.columns),
}

date_view = build_view(date_view_params)

date_view = build_views([date_view_params])[0]
```

## PostgreSQL specific

There is a PostgreSQL specific call that only returns when the PostgreSQL database
is available:

    # Wait until PostgreSQL is up
    # Library should be installed with hdx-python-database[postgresql]
    wait_for_postgresql("mydatabase", "myserver", 5432, "myuser", "mypass")
