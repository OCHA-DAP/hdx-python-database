"""View Tests"""

from sqlalchemy import column, select, table
from sqlalchemy.dialects import postgresql, sqlite

from hdx.database.views import CreateView, DropView


class TestViews:
    selectable = select(table("t", column("a")))

    def test_view_name_quoted(self):
        for dialect in (sqlite.dialect(), postgresql.dialect()):
            sql = str(CreateView("date_view", self.selectable).compile(dialect=dialect))
            assert sql.startswith("CREATE VIEW date_view AS SELECT")
            sql = str(DropView("date_view").compile(dialect=dialect))
            assert sql == "DROP VIEW date_view"

            name = 'x"; DROP TABLE t; --'
            sql = str(CreateView(name, self.selectable).compile(dialect=dialect))
            assert sql.startswith('CREATE VIEW "x""; DROP TABLE t; --" AS SELECT')
            sql = str(DropView(name).compile(dialect=dialect))
            assert sql == 'DROP VIEW "x""; DROP TABLE t; --"'
