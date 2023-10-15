"""
Create a DB View.

Copied from:
https://github.com/sqlalchemy/sqlalchemy/wiki/Views#sqlalchemy-14-20-version
"""
from typing import Dict, List

import sqlalchemy as sa
from sqlalchemy import MetaData, Selectable, TableClause
from sqlalchemy.ext import compiler
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table


class CreateView(DDLElement):
    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable


class DropView(DDLElement):
    def __init__(self, name):
        self.name = name


@compiler.compiles(CreateView)
def _create_view(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
    )


@compiler.compiles(DropView)
def _drop_view(element, compiler, **kw):
    return "DROP VIEW %s" % (element.name)


def view_exists(ddl, target, connection, **kw):
    return ddl.name in sa.inspect(connection).get_view_names()


def view_doesnt_exist(ddl, target, connection, **kw):
    return not view_exists(ddl, target, connection, **kw)


def view(name: str, metadata: MetaData, selectable: Selectable) -> TableClause:
    """Create SQLAlchemy view

    Args:
        name (str): View name
        metadata (MetaData): Base metadata
        selectable (Selectable): SQLAlchemy select statement

    Returns:
        TableClause: SQLAlchemy View
    """
    t = table(name)

    t._columns._populate_separate_keys(
        col._make_proxy(t) for col in selectable.selected_columns
    )

    sa.event.listen(
        metadata,
        "after_create",
        CreateView(name, selectable).execute_if(callable_=view_doesnt_exist),
    )
    sa.event.listen(
        metadata,
        "before_drop",
        DropView(name).execute_if(callable_=view_exists),
    )
    return t


def build_view(view_params: Dict) -> TableClause:
    """Create SQLAlchemy view from dictionary with keys: name, metadata and
    selectable

    Args:
        view_params (Dict): Dictionary with keys name, metadata, selectable

    Returns:
        TableClause: SQLAlchemy View
    """
    return view(**view_params)


def build_views(view_params_list: List[Dict]) -> List[TableClause]:
    """Create SQLAlchemy views from a list of dictionaries with keys: name,
    metadata and selectable

    Args:
        view_params_list (List[Dict]): List of dictionaries with view parameters
    Returns:
        List[TableClause]: SQLAlchemy Views
    """
    results = []
    for view_params in view_params_list:
        results.append(build_view(view_params))
    return results
