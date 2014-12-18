pgcontents
==========

A PostgreSQL-backed implementation of [IPEP 27](https://github.com/ipython/ipython/wiki/IPEP-27:-Contents-Service).

This repository is under development as part of the [Quantopian Research Environment](https://www.quantopian.com/research), currently in Alpha.

Getting Started
---------------
**Prerequisites:**
 - Write access to an empty [PostgreSQL](postgresql.org) database.
 - A Python installation with the latest master of [IPython Notebook](github.com/ipython/ipython).

**Installation:**

0. Install `pgcontents` from PyPI via `pip install pgcontents`.
1. Run `pgcontents init` to configure your database.  You will be prompted for a database URL for pgcontents to use for storage.
2. Configure IPython Notebook to use `PostgresContentsManager` as its storage backend.  This can be done from the command line or by modifying your `ipython_notebook_config.py` file.  See `examples/example_ipython_notebook_config.py` for an example config file.
3. Enjoy your filesystem-free IPython experience!
