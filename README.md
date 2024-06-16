**Reladiff** is a tool for comparing data across databases. It uses clever algorithms to push the diff computation to the databases themselves, making it super-fast. It is mainly aimed at data professionals, devops and sysadmins.

It is both a command-line tool, and a Python library.

It's free, open-source, simple to use, well-tested, and fast, even at massive scale.

Here are its main features:

 1. Fast Cross-Database Comparison: Compare data across different types of databases (e.g., MySQL to Snowflake). Reladiff uses a divide-and-conquer algorithm, based on comparing hashes, to optimally identify the modified segments, and only download the necessary data for comparison. It deals elegantly with reduced precision (e.g. timestamp(9) -> timestamp(3)), by rounding according to the spec of the database.

2. Fast Intra-Database Comparison: When both tables are in the same database, they will be compared using a join, with a few extra tricks to make it extra quick. It supports materializing the diff into a local table. It can also collect various extra statistics about the tables.

3. Threaded - Diffing using several threads gives a huge performance boost.

3. Configurable: Many switches for power-users to tinker with.

4. Automation-Friendly: Can output both JSON and a git-like diff. Easy to integrate into CI/CD pipelines.

5. Many databases supported: PostgreSQL, MySQL, Snowflake, BigQuery, Redshift, Oracle, Presto, Databricks, Trino, Clickhouse, Vertica, DuckDB

6. Works for tables with 10s of billions of rows

## Get Started

[**🗎 Read the Documentation**](https://reladiff.readthedocs.io/en/latest/) - our detailed documentation has everything you need to start diffing.

## For the impatient

### Install

Reladiff is available on [PyPI](https://pypi.org/project/reladiff/). You may install it by running:

```
pip install reladiff
```

We advise to install it within a virtual-env.

### How to Use

Once you've installed `reladiff`, you can run it from the command-line:

```
reladiff DB1_URI TABLE1_NAME DB2_URI TABLE2_NAME [OPTIONS]
```

When both tables belong to the same database, a shorter syntax is availble:

```
reladiff DB1_URI TABLE1_NAME TABLE2_NAME [OPTIONS]
```

Diffing within the same database is also faster, as it uses a join-based algorithm.

Or, you can run it from Python:
```python
from reladiff import connect_to_table, diff_tables

table1 = connect_to_table("postgresql:///", "table_name", "id")
table2 = connect_to_table("mysql:///", "table_name", "id")

sign: Literal['+' | '-']
row: tuple[str, ...]
for sign, row in diff_tables(table1, table2):
    print(sign, row)
```

Read our detailed instructions:

* [How to use from the shell (or: command-line)](https://reladiff.readthedocs.io/en/latest/how-to-use.html#how-to-use-from-the-shell-or-command-line)
* [How to use from Python](https://reladiff.readthedocs.io/en/latest/how-to-use.html#how-to-use-from-python)
* [How to use with TOML configuration file](https://reladiff.readthedocs.io/en/latest/how-to-use.html#how-to-use-with-a-configuration-file)


#### "Real-world" example: Diff "events" table between Postgres and Snowflake

```
reladiff \
  postgresql:/// \
  events \
  "snowflake://<username>:<password>@<password>/<DATABASE>/<SCHEMA>?warehouse=<WAREHOUSE>&role=<ROLE>" \
  events \
  -k event_id \         # Identifier of event
  -c event_data \       # Extra column to compare
  -w "event_time < '2024-10-10'"    # Filter the rows on both dbs
```

#### "Real-world" example: Diff "events" and "old_events" tables in the same Postgres DB

Materializes the results into a new table, containing the current timestamp in its name.

```
reladiff \
  postgresql:///  events  old_events \
  -k org_id \
  -c created_at -c is_internal \
  -w "org_id != 1 and org_id < 2000" \
  -m test_results_%t \
  --materialize-all-rows \
  --table-write-limit 10000
```

#### More examples

<p align="center">
  <img alt="diff2" src="https://user-images.githubusercontent.com/1799931/196754998-a88c0a52-8751-443d-b052-26c03d99d9e5.png" />
</p>

<p align="center">
  <a href=https://www.loom.com/share/682e4b7d74e84eb4824b983311f0a3b2 target="_blank">
    <img alt="Intro to Diff" src="https://user-images.githubusercontent.com/1799931/196576582-d3535395-12ef-40fd-bbbb-e205ccae1159.png" width="50%" height="50%" />
  </a>
</p>


### Technical Explanation

Check out this [technical explanation](https://reladiff.readthedocs.io/en/latest/technical-explanation.html) of how reladiff works.

### We're here to help!

* Confused? Got a cool idea? Just want to share your thoughts? Let's discuss it in [GitHub Discussions](https://github.com/erezsh/reladiff/discussions).

* Did you encounter a bug? [Open an issue](https://github.com/erezsh/reladiff/issues).

## How to Contribute
* Please read the [contributing guidelines](https://github.com/erezsh/reladiff/blob/master/CONTRIBUTING.md) to get started.
* Feel free to open an issue or contribute to the project by working on an existing issue.

Big thanks to everyone who contributed so far:

<a href="https://github.com/erezsh/reladiff/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=erezsh/reladiff" />
</a>


## License

This project is licensed under the terms of the [MIT License](https://github.com/erezsh/reladiff/blob/master/LICENSE).
