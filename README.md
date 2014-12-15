pgcontents
==========

A PostgreSQL-backed implementation of [IPEP 27](https://github.com/ipython/ipython/wiki/IPEP-27:-Contents-Service).

This repository is under development as part of the [Quantopian Research Environment](https://www.quantopian.com/research), currently in Alpha.

Getting Started
---------------
0. Clone this repo via `git clone git@github.com:quantopian/pgcontents.git.
1. Install dependencies via `pip install -r requirements.txt.
2. Edit `pgcontents/alembic.ini` and point `sqlalchemy.url` to a postgres database you have read/write access to.
3. Create/upgrade your database schema to the most recent migration by running: `alembic upgrade head`.
4. Run the tests with `nosetests pgcontents/tests`.
5. ???
6. Profit!
