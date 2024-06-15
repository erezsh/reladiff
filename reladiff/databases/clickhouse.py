from sqeleton.databases import clickhouse
from .base import ReladiffDialect


class Dialect(clickhouse.Dialect, clickhouse.Mixin_MD5, clickhouse.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Clickhouse(clickhouse.Clickhouse):
    dialect = Dialect()
