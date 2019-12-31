PGContents
==========

PGContents is a PostgreSQL-backed implementation of `IPEP 27 <https://github.com/ipython/ipython/wiki/IPEP-27:-Contents-Service>`_.  It aims to a be a transparent, drop-in replacement for IPython's standard filesystem-backed storage system.  PGContents' `PostgresContentsManager` class can be used to replace all local filesystem storage with database-backed storage, while its `PostgresCheckpoints` class can be used to replace just IPython's checkpoint storage.  These features are useful when running IPython in environments where you either don't have access to—or don't trust the reliability of—the local filesystem of your notebook server.

This repository developed as part of the `Quantopian Research Environment <https://www.quantopian.com/research>`_.

Getting Started
---------------
**Prerequisites:**
 - Write access to an empty `PostgreSQL <http://www.postgresql.org>`_ database.
 - A Python installation with `Jupyter Notebook <https://github.com/jupyter/notebook>`_ >= 5.0.

**Installation:**

0. Install ``pgcontents`` from PyPI via ``pip install pgcontents``.
1. Run ``pgcontents init`` to configure your database.  You will be prompted for a database URL for pgcontents to use for storage.  (Alternatively, you can set the ``PGCONTENTS_DB_URL`` environment variable, or pass ``--db-url`` on the command line).
2. Configure Jupyter to use pgcontents as its storage backend.  This can be done from the command line or by modifying your notebook config file. On a Unix-like system, your notebook config will be located at ``~/.jupyter/jupyter_notebook_config.py``. See the ``examples`` directory for example configuration files.
3. Enjoy your filesystem-free Jupyter experience!

Demo Video
----------
You can see a demo of PGContents in action in `this presentation from JupyterCon 2017`_.

.. _`this presentation from JupyterCon 2017` : https://youtu.be/TtsbspKHJGo?t=917
