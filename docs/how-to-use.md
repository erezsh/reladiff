# User guide

Once you've [installed](https://reladiff.readthedocs.io/en/latest/install.html) Reladiff, you can run it from the command-line, or from Python.

## How to use from the shell / command-line

The basic syntax for reladiff is:

```bash
# Cross-DB diff, using hashes
reladiff  DB1_URI  TABLE1_NAME  DB2_URI  TABLE2_NAME  [OPTIONS]
```

When both tables belong to the same database, a shorter syntax is availble:

```bash
# Same-DB diff, using outer join
reladiff  DB_URI  TABLE1_NAME  TABLE2_NAME  [OPTIONS]
```

`DB_URL` is either a [database URL](supported-databases.md), or the name of a database definition that is specified in a [configuration file](https://reladiff.readthedocs.io/en/latest/how-to-use.html#how-to-use-with-a-configuration-file). Our database URLs conform to the same format as SQLAlchemy.

We recommend using a configuration file, with the ``--conf`` switch, to keep the command simple and manageable.

For a list of example URLs, see [list of supported databases](supported-databases.md).

Note: Because URLs allow many special characters, and may collide with the syntax of your shell,
it's recommended to surround them with quotes.

### Options

  - `--help` - Show help message and exit.
  - `-k` or `--key-columns` - Name of the primary key column. If none provided, default is 'id'.
  - `-t` or `--update-column` - Name of updated_at/last_updated column
  - `-c` or `--columns` - Names of extra columns to compare.  Can be used more than once in the same command.
                          Accepts a name or a pattern like in SQL.
                          Example: `-c col% -c another_col -c %foorb.r%`
  - `-l` or `--limit` - Maximum number of differences to find (limits maximum bandwidth and runtime)
  - `-s` or `--stats` - Print stats instead of a detailed diff
  - `-d` or `--debug` - Print debug info
  - `-v` or `--verbose` - Print extra info
  - `-i` or `--interactive` - Confirm queries, implies `--debug`
  - `--json` - Print JSONL output for machine readability
  - `--min-age` - Considers only rows older than specified. Useful for specifying replication lag.
                  Example: `--min-age=5min` ignores rows from the last 5 minutes.
                  Valid units: `d, days, h, hours, min, minutes, mon, months, s, seconds, w, weeks, y, years`
  - `--max-age` - Considers only rows younger than specified. See `--min-age`.
  - `-j` or `--threads` - Number of worker threads to use per database. Default=1.
  - `-w`, `--where` - An additional 'where' expression to restrict the search space.
  - `--conf`, `--run` - Specify the run and configuration from a TOML file. (see below)
  - `--bisection-threshold` - Minimal size of segment to be split. Smaller segments will be downloaded and compared locally.
  - `--bisection-factor` - Segments per iteration. When set to 2, it performs binary search.
  - `-m`, `--materialize` - Materialize the diff results into a new table in the database.
                            If a table exists by that name, it will be replaced.
                            Use `%t` in the name to place a timestamp.
                            Example: `-m test_mat_%t`
  - `--assume-unique-key` - Skip validating the uniqueness of the key column during joindiff, which is costly in non-cloud dbs.
  - `--sample-exclusive-rows` - Sample several rows that only appear in one of the tables, but not the other. Use with `-s`.
  - `--materialize-all-rows` -  Materialize every row, even if they are the same, instead of just the differing rows.
  - `--table-write-limit` - Maximum number of rows to write when creating materialized or sample tables, per thread. Default=1000.
  - `-a`, `--algorithm` `[auto|joindiff|hashdiff]` - Force algorithm choice



### How to use with a configuration file

Reladiff lets you load the configuration for a run from a TOML file.

**Reasons to use a configuration file:**

- Convenience: Set-up the parameters for diffs that need to run often

- Easier and more readable: You can define the database connection settings as separate config values, instead of in a single URI.

- Gives you fine-grained control over the settings switches, without requiring any Python code.

Use `--conf` to specify that path to the configuration file. reladiff will load the settings from `run.default`, if it's defined.

Then you can, optionally, use `--run` to choose to load the settings of a specific run, and override the settings `run.default`. (all runs extend `run.default`, like inheritance).

Finally, CLI switches have the final say, and will override the settings defined by the configuration file, and the current run.

Example TOML file:

```toml
# Specify the connection params to the test database.
[database.test_postgresql]
driver = "postgresql"
user = "postgres"
password = "Password1"

# Specify the default run params
[run.default]
update_column = "timestamp"
verbose = true

# Specify params for a run 'test_diff'.
[run.test_diff]
verbose = false
# Source 1 ("left")
1.database = "test_postgresql"                      # Use options from database.test_postgresql
1.table = "rating"
# Source 2 ("right")
2.database = "postgresql://postgres:Password1@/"    # Use URI like in the CLI
2.table = "rating_del1"
```

In this example, running `reladiff --conf myconfig.toml --run test_diff` will compare between `rating` and `rating_del1`.
It will use the `timestamp` column as the update column, as specified in `run.default`. However, it won't be verbose, since that
flag is overwritten to `false`.

Running it with `reladiff --conf myconfig.toml --run test_diff -v` will set verbose back to `true`.


## How to use from Python

Import the `reladiff` module, and use the following functions:

- `connect_to_table()` to connect to a specific table in the database

- `diff_tables()` to diff those tables


Example:

```python
# Optional: Set logging to display the progress of the diff
import logging
logging.basicConfig(level=logging.INFO)

from reladiff import connect_to_table, diff_tables

table1 = connect_to_table("postgresql:///", "table_name", "id")
table2 = connect_to_table("mysql:///", "table_name", "id")

sign: Literal['+' | '-']
row: tuple[str, ...]
for sign, row in diff_tables(table1, table2):
    print(sign, row)
```

To learn more about the different options, [read the API reference](https://reladiff.readthedocs.io/en/latest/python-api.html) or run `help(diff_tables)`.


## Tips

- If you only care for a boolean (yes/no) response, set `--limit=1` for a much faster result.

- Setting a higher thread count may help performance significantly, depending on the database.

- a low `--bisection-threshold` will minimize the amount of network transfer. But if network isn't an issue, a high `--bisection-threshold` will make Reladiff run a lot faster.
