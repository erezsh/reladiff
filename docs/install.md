# How to install

## Install library and CLI (no drivers)

Reladiff is available on [PyPI](https://pypi.org/project/reladiff/). You may install it by running:

```sh
pip install reladiff
```

(Note: Make sure `pip` is installed first)

## Install with database drivers

You may install the necessary database drivers, at the same time as when installing Reladiff, using pip's "extra" syntax.

We advise to install Reladiff within a virtual-env, because the drivers may bring many dependencies.

```sh
# Install all database drivers
pip install reladiff[all]

# The above line is equivalent to:
pip install reladiff[duckdb,mysql,postgresql,snowflake,presto,oracle,trino,clickhouse,vertica]
```

You may remove all the databases you don't plan to use.

For example, if you only want to diff between Postgresql and DuckDB, install Reladiff thusly:

```sh
pip install reladiff[duckdb,postgresql]
```

### Notes for shell / command-line

In some shells, like `bash` and `powershell`, you will have to use quotes, in order to allow the `[]` syntax.

For example:

```sh
pip install 'reladiff[all]'     # will work on bash
pip install "reladiff[all]"     # will work on powershell (Windows)
```

Consult your shell environment to learn the correct way to quote or escape your command.

### Notes for BigQuery

Reladiff currently doesn't auto-install the BigQuery drivers.

For BigQuery, see: [https://pypi.org/project/google-cloud-bigquery](https://pypi.org/project/google-cloud-bigquery)


### Another way to install all the drivers

For your convenience, you may also run these commands one after the other. You may omit drivers that you don't plan to use.

```bash
pip install reladiff[duckdb]
pip install reladiff[mysql]
pip install reladiff[postgresql]
pip install reladiff[snowflake]
pip install reladiff[presto]
pip install reladiff[oracle]
pip install reladiff[trino]
pip install reladiff[clickhouse]
pip install reladiff[vertica]
```
