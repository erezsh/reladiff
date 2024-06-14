from sqeleton.databases import postgresql as pg
from .base import ReladiffDialect


class PostgresqlDialect(pg.PostgresqlDialect, pg.Mixin_MD5, pg.Mixin_NormalizeValue, ReladiffDialect):
    pass


class PostgreSQL(pg.PostgreSQL):
    dialect = PostgresqlDialect()
