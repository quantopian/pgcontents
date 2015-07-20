PGContents
==========

PGContents is a PostgreSQL-backed implementation of `IPEP 27 <https://github.com/ipython/ipython/wiki/IPEP-27:-Contents-Service>`_.  It aims to a be a transparent, drop-in replacement for IPython's standard filesystem-backed storage system.  PGContents' `PostgresContentsManager` class can be used to replace all local filesystem storage with database-backed storage, while its `PostgresCheckpoints` class can be used to replace just IPython's checkpoint storage.  These features are useful when running IPython in environments where you either don't have access to—or don't trust the reliability of—the local filesystem of your notebook server.

This repository is under development as part of the `Quantopian Research Environment <https://www.quantopian.com/research>`_, currently in Open Beta.

Getting Started
---------------
**Prerequisites:**
 - Write access to an empty `PostgreSQL <http://www.postgresql.org>`_ database.
 - A Python installation with `IPython <https://github.com/ipython/ipython>`_ 3.2.x.

**Installation:**

0. Install `pgcontents` from PyPI via `pip install pgcontents`.
1. Run `pgcontents init` to configure your database.  You will be prompted for a database URL for pgcontents to use for storage.
2. Configure IPython Notebook to use pgcontents as its storage backend.  This can be done from the command line or by modifying your `ipython_notebook_config.py` file.  For a standard IPython installation on Unix-like systems, your profile will be located located at `~/.ipython/profile_default/ipython_notebook_config.py`. See the `examples` directory for example configuration files.
3. Enjoy your filesystem-free IPython experience!
