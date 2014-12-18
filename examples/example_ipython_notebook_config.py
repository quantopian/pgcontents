c = get_config()

# Tell IPython to use PostgresContentsManager.
c.NotebookApp.contents_manager_class = 'pgcontents.pgmanager.PostgresContentsManager'

# Set the url for the database used to store files.  See
# http://docs.sqlalchemy.org/en/rel_0_9/core/engines.html#postgresql
# for more info on db url formatting.
# c.PostgresContentsManager.db_url = 'postgresql://ssanderson@/pgcontents'

# Set a user ID. Defaults to the result of getpass.getuser()
# c.PostgresContentsManager.user_id = 'my_awesome_username'

# Set a maximum file size, if desired.
# c.PostgresContentsManager.max_file_size_bytes = 1000000 # 1MB File cap
