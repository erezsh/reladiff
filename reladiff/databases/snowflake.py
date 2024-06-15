from sqeleton.databases import snowflake
from .base import ReladiffDialect


class Dialect(snowflake.Dialect, snowflake.Mixin_MD5, snowflake.Mixin_NormalizeValue, ReladiffDialect):
    pass


class Snowflake(snowflake.Snowflake):
    dialect = Dialect()
