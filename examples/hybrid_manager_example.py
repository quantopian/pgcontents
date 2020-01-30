# This example shows how to configure Jupyter/IPython to use the more complex
# HybridContentsManager.

# A HybridContentsManager implements the contents API by delegating requests to
# other contents managers. Each sub-manager is associated with a root
# directory, and all requests for data within that directory are routed to the
# sub-manager.

# A HybridContentsManager needs two pieces of information at configuration
# time:

# 1. ``manager_classes``, a map from root directory to the type of contents
#    manager to use for that root directory.
# 2. ``manager_kwargs``, a map from root directory to a dict of keywords to
#    pass to the associated sub-manager.

from pgcontents.pgmanager import PostgresContentsManager
from pgcontents.hybridmanager import HybridContentsManager

# Using Jupyter (IPython >= 4.0).
# from notebook.services.contents.filemanager import FileContentsManager
# Using Legacy IPython.
from IPython.html.services.contents.filemanager import FileContentsManager

c = get_config()  # noqa

c.NotebookApp.contents_manager_class = HybridContentsManager
c.HybridContentsManager.manager_classes = {
    # Associate the root directory with a PostgresContentsManager.
    # This manager will receive all requests that don't fall under any of the
    # other managers.
    '': PostgresContentsManager,
    # Associate /directory with a FileContentsManager.
    'directory': FileContentsManager,
    # Associate /other_directory with another FileContentsManager.
    'other_directory': FileContentsManager,
}
c.HybridContentsManager.manager_kwargs = {
    # Args for root PostgresContentsManager.
    '': {
        'db_url': 'postgresql://ssanderson@/pgcontents_testing',
        'user_id': 'my_awesome_username',
        'max_file_size_bytes': 1000000,  # Optional
    },
    # Args for the FileContentsManager mapped to /directory
    'directory': {
        'root_dir': '/home/ssanderson/some_local_directory',
    },
    # Args for the FileContentsManager mapped to /other_directory
    'other_directory': {
        'root_dir': '/home/ssanderson/some_other_local_directory',
    }
}
