from sqeleton.databases import mysql
from .base import ReladiffDialect


class Dialect(mysql.Dialect, mysql.Mixin_MD5, mysql.Mixin_NormalizeValue, ReladiffDialect):
    pass


class MySQL(mysql.MySQL):
    dialect = Dialect()
