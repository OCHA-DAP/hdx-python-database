from hdx.database import Database


def test_recreate_schema(mock_engine):
    db_uri = "postgresql+psycopg://myuser:mypass@0.0.0.0:12345/mydatabase"
    assert Database.recreate_schema(mock_engine, db_uri) is True
    db_uri = "Error"
    assert Database.recreate_schema(mock_engine, db_uri) is False
