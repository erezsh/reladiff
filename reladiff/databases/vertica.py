from sqeleton.databases import vertica
from .base import ReladiffDialect


class Dialect(vertica.Dialect, vertica.Mixin_MD5, vertica.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Vertica(vertica.Vertica):
    dialect = Dialect()
