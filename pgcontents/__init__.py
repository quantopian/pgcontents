# Do this first so that we bail early with a useful message if the user didn't
# specify [ipy3] or [ipy4].
try:
    import IPython  # noqa
except ImportError:
    raise ImportError(
        "No IPython installation found.\n"
        "To install pgcontents with the latest Jupyter Notebook"
        " run 'pip install pgcontents[ipy4]b'.\n"
        "To install with the legacy IPython Notebook"
        " run 'pip install pgcontents[ipy3]'.\n"
    )

from .checkpoints import PostgresCheckpoints
from .pgmanager import PostgresContentsManager

__all__ = [
    'PostgresCheckpoints',
    'PostgresContentsManager',
]
