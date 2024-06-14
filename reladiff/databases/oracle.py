from sqeleton.databases import oracle
from .base import ReladiffDialect


class Dialect(oracle.Dialect, oracle.Mixin_MD5, oracle.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Oracle(oracle.Oracle):
    dialect = Dialect()
