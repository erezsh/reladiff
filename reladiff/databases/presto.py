from sqeleton.databases import presto
from .base import ReladiffDialect


class Dialect(presto.Dialect, presto.Mixin_MD5, presto.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Presto(presto.Presto):
    dialect = Dialect()
