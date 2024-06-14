.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   supported-databases
   how-to-use
   python-api
   technical-explanation
   new-database-driver-guide

Introduction
------------

**reladiff** is a command-line tool and Python library to efficiently diff
rows across two different databases.

⇄  Verifies across many different databases (e.g. *PostgreSQL* -> *Snowflake*) !

🔍 Outputs diff of rows in detail

🚨 Simple CLI/API to create monitoring and alerts

🔥 Verify 25M+ rows in <10s, and 1B+ rows in ~5min.

♾️  Works for tables with 10s of billions of rows

For more information, `See our README <https://github.com/erezsh/reladiff#readme>`_

How to install
--------------

Requires Python 3.7+ with pip.

::

    pip install reladiff

For installing with 3rd-party database connectors, use the following syntax:

::

    pip install "reladiff[db1,db2]"

    e.g.
    pip install "reladiff[mysql,postgresql]"

Supported connectors:

- mysql
- postgresql
- snowflake
- presto
- oracle
- trino
- clickhouse
- vertica



Resources
---------

- Source code (git): `<https://github.com/erezsh/reladiff>`_

- User Documentation
    - :doc:`supported-databases`
    - :doc:`how-to-use`
    - :doc:`python-api`
    - :doc:`technical-explanation`
- Contributor Documentation
   - :doc:`new-database-driver-guide`
