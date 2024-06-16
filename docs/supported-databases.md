# List of supported databases

| Database      | Status | Connection string |
|---------------|-------------------------------------------------------------------------------------------------------------------------------------|--------|
| PostgreSQL >=10 |  💚    | `postgresql://<user>:<password>@<host>:5432/<database>`          |
| MySQL         |  💚    | `mysql://<user>:<password>@<hostname>:5432/<database>`             |
| Snowflake     |  💚    | `"snowflake://<user>[:<password>]@<account>/<database>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<role>[&authenticator=externalbrowser]"` |
| Redshift      |  💚    | `redshift://<username>:<password>@<hostname>:5439/<database>`    |
| DuckDB >= 0.6       |  💚    | `duckdb://<file>`  |
| BigQuery      |  💛    | `bigquery://<project>/<dataset>`                                |
| Oracle        |  💛    | `oracle://<username>:<password>@<hostname>/database`            |
| Presto        |  💛    | `presto://<username>:<password>@<hostname>:8080/<database>`     |
| Trino         |  💛    | `trino://<username>:<password>@<hostname>:8080/<database>`      |
| Vertica       |  💛    | `vertica://<username>:<password>@<hostname>:5433/<database>`   |
| Trino         |  💛    | `trino://<username>:<password>@<hostname>:8080/<database>`      |
| Clickhouse    |  💛    | `clickhouse://<username>:<password>@<hostname>:9000/<database>` |
| Databricks    |  💛    | `databricks://<http_path>:<access_token>@<server_hostname>/<catalog>/<schema>`     |
| ElasticSearch |  📝    |   |
| Planetscale   |  📝    |   u                                                                                                                                 |
| Pinot         |  📝    |                                                                                                                                     |
| Druid         |  📝    |                                                                                                                                     |
| Kafka         |  📝    |                                                                                                                                     |
| SQLite        |  📝    |                                                                                                                                     |

* 💚: Implemented and thoroughly tested.
* 💛: Implemented, but not thoroughly tested yet.
* ⏳: Implementation in progress.
* 📝: Implementation planned. Contributions welcome.


#### Looking for a database not on the list?
If a database is not on the list, we'd still love to support it. [Please open an issue](https://github.com/erezsh/reladiff/issues) to discuss it, or vote on existing requests to push them up our todo list.

We also accept pull-requests!
