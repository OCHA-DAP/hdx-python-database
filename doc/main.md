# Summary

The HDX Python Database Library provides utilities for connecting to databases in a standard way including
through an ssh tunnel if needed. It is built on top of SQLAlchemy and simplifies its setup.

# Information

This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.

The code for the library is [here](https://github.com/OCHA-DAP/hdx-python-database).
The library has detailed API documentation which can be found in the menu at the top. 

Additional postgresql functionality is available if this library is installed with extra "postgresql":

    pip install hdx-python-database[postgresql]

## Breaking changes

From 1.2.1, wait_for_postgresql takes connection URI not database parameters, 
get_params_from_sqlalchemy_url renamed to get_params_from_connection_uri
and moved to dburi module, get_sqlalchemy_url renamed to get_connection_uri and 
moved to dburi module. New function remove_driver_from_uri in dburi module.
Parameter driver replaced by dialect+driver. 

From 1.1.2, the postgres module is renamed postgresql and function wait_for_postgres
is renamed wait_for_postgresql.

Versions from 1.0.8 support Python 3.6 and later.

Versions from 1.0.6 no longer support Python 2.7. 

# Description of Utilities

## Database

Your SQLAlchemy database tables must inherit from Base in
hdx.utilities.database eg. :

    from hdx.database import Base
    class MyTable(Base):
        my_col = Column(Integer, ForeignKey(MyTable2.col2), primary_key=True)

Then a connection can be made to a database as follows including through an SSH
tunnel:

    # Get SQLAlchemy session object given database parameters and
    # if needed SSH parameters. If database is PostgreSQL, will poll
    # till it is up.
    from hdx.database import Database
    with Database(database="db", host="1.2.3.4", username="user", 
                  password="pass", dialect="dialect", driver="driver", 
                  ssh_host="5.6.7.8", ssh_port=2222, ssh_username="sshuser", 
                  ssh_private_key="path_to_key") as session:
        session.query(...)

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

## PostgreSQL specific

There is a PostgreSQL specific call that only returns when the PostgreSQL database
is available:

    # Wait until PostgreSQL is up
    # Library should be installed with hdx-python-database[postgresql]
    wait_for_postgresql("mydatabase", "myserver", 5432, "myuser", "mypass")

