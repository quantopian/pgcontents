from pgcontents import PostgresCheckpoints
c = get_config()

# Tell IPython to use PostgresCheckpoints for checkpoint storage.
c.NotebookApp.checkpoints_class = PostgresCheckpoints

# Set the url for the database used to store files.  See
# http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#postgresql
# for more info on db url formatting.
c.PostgresContentsManager.db_url = 'postgresql://ssanderson:secret_password@myhost.org:5432/pgcontents'
