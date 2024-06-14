from sqeleton.databases import bigquery
from .base import ReladiffDialect


class Dialect(bigquery.Dialect, bigquery.Mixin_MD5, bigquery.Mixin_NormalizeValue, ReladiffDialect):
    pass


class BigQuery(bigquery.BigQuery):
    dialect = Dialect()
