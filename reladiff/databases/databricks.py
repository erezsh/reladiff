from sqeleton.databases import databricks
from .base import ReladiffDialect


class Dialect(databricks.Dialect, databricks.Mixin_MD5, databricks.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Databricks(databricks.Databricks):
    dialect = Dialect()
