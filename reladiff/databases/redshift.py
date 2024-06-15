from sqeleton.databases import redshift
from .base import ReladiffDialect


class Dialect(redshift.Dialect, redshift.Mixin_MD5, redshift.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Redshift(redshift.Redshift):
    dialect = Dialect()
