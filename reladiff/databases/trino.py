from sqeleton.databases import trino
from .base import ReladiffDialect


class Dialect(trino.Dialect, trino.Mixin_MD5, trino.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Trino(trino.Trino):
    dialect = Dialect()
