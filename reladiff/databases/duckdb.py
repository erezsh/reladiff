from sqeleton.databases import duckdb
from .base import ReladiffDialect


class Dialect(duckdb.Dialect, duckdb.Mixin_MD5, duckdb.Mixin_NormalizeValue, ReladiffDialect):
    pass


class DuckDB(duckdb.DuckDB):
    dialect = Dialect()
