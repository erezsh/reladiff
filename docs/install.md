# How to install

Reladiff is available on [PyPI](https://pypi.org/project/reladiff/). You may install it by running:

```
pip install reladiff
```

(Note: Make sure `pip` is installed first)

We advise to install `reladiff` within a virtual-env, because the drivers for  it may bring many dependencies.

Reladiff is available through Python's package manager:

```
pip install reladiff
```


#### Then, install one or more driver(s) specific to the database(s) you want to connect to.

- `pip install 'reladiff[mysql]'`

- `pip install 'reladiff[postgresql]'`

- `pip install 'reladiff[snowflake]'`

- `pip install 'reladiff[presto]'`

- `pip install 'reladiff[oracle]'`

- `pip install 'reladiff[trino]'`

- `pip install 'reladiff[clickhouse]'`

- `pip install 'reladiff[vertica]'`

- For BigQuery, see: https://pypi.org/project/google-cloud-bigquery/

_Some drivers have dependencies that cannot be installed using `pip` and still need to be installed manually._


We advise to install it within a virtual-env.

$$$$ SAY ABOUT IT, poetry etc.  $$$$
