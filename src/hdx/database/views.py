"""
Create a DB View.

Copied from:
https://github.com/sqlalchemy/sqlalchemy/wiki/Views#sqlalchemy-14-20-version
"""
from dataclasses import dataclass
from typing import List

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


@dataclass
class ViewParams:
    """Class for keeping view constructor parameters."""

    name: str
    metadata: MetaData
    selectable: Selectable


def build_view(view_params: ViewParams) -> TableClause:
    """Create SQLAlchemy view

    Args:
        view_params (ViewParams): ViewParams object

    Returns:
        TableClause: SQLAlchemy View
    """
    return view(**view_params.__dict__)


def build_views(view_params_list: List[ViewParams]) -> List[TableClause]:
    """Create SQLAlchemy views from a list of ViewParams objects

    Args:
        view_params_list (List[ViewParams]): List of ViewParams objects

    Returns:
        List[TableClause]: SQLAlchemy Views
    """
    results = []
    for view_params in view_params_list:
        results.append(build_view(view_params))
    return results
