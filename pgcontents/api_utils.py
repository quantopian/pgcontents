"""
Utilities for implementing the ContentsManager API.
"""


def from_api_dirname(api_dirname):
    """
    Convert API-style directory name into the format stored in the database.

    TODO: Consider implementgin this with a SQLAlchemy TypeDecorator.
    """
    # Special case for root directory.
    if api_dirname == '':
        return '/'
    return ''.join(
        [
            '' if api_dirname.startswith('/') else '/',
            api_dirname,
            '' if api_dirname.endswith('/') else '/',
        ]
    )


def to_api_path(db_path):
    """
    Convert database path into API-style path.

    TODO: Consider implementing this with a SQLAlchemy TypeDecorator.
    """
    return db_path.strip('/')


def split_api_filepath(path):
    """
    Split an API file path into directory and name.
    """
    parts = path.rsplit('/', 1)
    if len(parts) == 1:
        name = parts[0]
        dirname = '/'
    else:
        name = parts[1]
        dirname = parts[0] + '/'

    return from_api_dirname(dirname), name
