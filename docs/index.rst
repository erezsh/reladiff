.. toctree::
   :maxdepth: 2
   :caption: Reference
   :hidden:

   install
   how-to-use
   supported-databases
   python-api
   technical-explanation
   new-database-driver-guide

Reladiff
------------

**Reladiff** is a high-performance tool and library designed for diffing large datasets across databases. By executing the diff calculation within the database itself, Reladiff minimizes data transfer and achieves optimal performance.

This tool is specifically tailored for data professionals, DevOps engineers, and system administrators.

Reladiff is free, open-source, user-friendly, extensively tested, and delivers fast results, even at massive scale.

Key Features
============

1. **Cross-Database Diff**: *Reladiff* employs a divide-and-conquer algorithm, based on matching hashes, to efficiently identify modified segments and download only the necessary data for comparison. This approach ensures exceptional performance when differences are minimal.

   - ⇄ Diffs across over a dozen different databases (e.g. *PostgreSQL* -> *Snowflake*)!

   - 🧠 Gracefully handles reduced precision (e.g., timestamp(9) -> timestamp(3)) by rounding according to the database specification.

   - 🔥 Benchmarked to diff over 25M rows in under 10 seconds and over 1B rows in approximately 5 minutes, given no differences.

   - ♾️ Capable of handling tables with tens of billions of rows.

2. **Intra-Database Diff**: When both tables reside in the same database, Reladiff compares them using a join operation, with additional optimizations for enhanced speed.

   - Supports materializing the diff into a local table.
   - Can collect various extra statistics about the tables.

3. **Threaded**: Utilizes multiple threads to significantly boost performance during diffing operations.

4. **Configurable**: Offers numerous options for power-users to customize and optimize their usage.

5. **Automation-Friendly**: Outputs both JSON and git-like diffs (with + and -), facilitating easy integration into CI/CD pipelines.

6. **Over a dozen databases supported**: MySQL, Postgres, Snowflake, Bigquery, Oracle, Clickhouse, and more. `See full list <https://reladiff.readthedocs.io/en/latest/supported-databases.html>`_.

Reladiff is a fork of an archived project called `data-diff <https://github.com/datafold/data-diff>`_. Code that worked with data-diff should also work with reladiff, without any changes. However, there are a few differences: Reladiff doesn't contain any tracking code. Reladiff doesn't have DBT integration.

Resources
---------


- User Documentation
    - :doc:`install`
    - :doc:`how-to-use`
    - :doc:`supported-databases`
    - :doc:`python-api`
    - :doc:`technical-explanation`
- Developer Documentation
   - :doc:`new-database-driver-guide`
    - In-depth technical blog post: `<https://eshsoft.com/blog/how-reladiff-works>`_

- Other links
    - Github: `<https://github.com/erezsh/reladiff>`_
