[![Build Status](https://github.com/OCHA-DAP/hdx-python-database/workflows/build/badge.svg)](https://github.com/OCHA-DAP/hdx-python-database/actions?query=workflow%3Abuild) [![Coverage Status](https://coveralls.io/repos/github/OCHA-DAP/hdx-python-database/badge.svg?branch=master&ts=1)](https://coveralls.io/github/OCHA-DAP/hdx-python-database?branch=master)

The HDX Python Database Library provides utilities for connecting to databases in a standardised way including
through an ssh tunnel if needed. It is built on top of SQLAlchemy and simplifies its setup. Additional postgres
functionality is available if this library is installed with:

    pip install hdx-python-database[postgres]


This library is part of the [Humanitarian Data Exchange](https://data.humdata.org/) (HDX) project. If you have 
humanitarian related data, please upload your datasets to HDX.

  - [Usage](#usage)
  - [Examples](#examples)

## Breaking changes
Versions from 1.0.6 no longer support Python 2.7. 

## Usage

The library has detailed API documentation which can be found here: <http://ocha-dap.github.io/hdx-python-database/>. 
The code for the library is here: <https://github.com/ocha-dap/hdx-python-database>.

### Examples

Your SQLAlchemy database tables must inherit from Base in
hdx.utilities.database eg. :

    from hdx.database import Base
    class MyTable(Base):
        my_col = Column(Integer, ForeignKey(MyTable2.col2), primary_key=True)

Examples:

    # Get SQLAlchemy session object given database parameters and
    # if needed SSH parameters. If database is PostgreSQL, will poll
    # till it is up.
    from hdx.database import Database
    with Database(database='db', host='1.2.3.4', username='user', password='pass',
                  driver='driver', ssh_host='5.6.7.8', ssh_port=2222,
                  ssh_username='sshuser', ssh_private_key='path_to_key') as session:
        session.query(...)

    # Extract dictionary of parameters from SQLAlchemy url
    result = Database.get_params_from_sqlalchemy_url(TestDatabase.sqlalchemy_url)

    # Build SQLAlchemy url from dictionary of parameters
    result = Database.get_sqlalchemy_url(**TestDatabase.params)

    # Wait util PostgreSQL is up
    # Library should be installed with hdx-python-database[postgres]
    wait_for_postgres('mydatabase', 'myserver', 5432, 'myuser', 'mypass')

