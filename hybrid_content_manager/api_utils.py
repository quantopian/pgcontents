"""
Utilities for implementing the ContentsManager API.
"""
from __future__ import unicode_literals

from datetime import datetime
from functools import wraps
import posixpath

from tornado.web import HTTPError


class PathOutsideRoot(Exception):
    pass


# We don't currently track created/modified dates for directories, so this
# value is always used instead.
DUMMY_CREATED_DATE = datetime.fromtimestamp(0)


def base_model(path):
    return {
        "name": path.rsplit('/', 1)[-1],
        "path": path,
        "writable": True,
        "last_modified": None,
        "created": None,
        "content": None,
        "format": None,
        "mimetype": None,
    }


def base_directory_model(path):
    m = base_model(path)
    m.update(
        type='directory',
        last_modified=DUMMY_CREATED_DATE,
        created=DUMMY_CREATED_DATE,
    )
    return m


def normalize_api_path(api_path):
    """
    Resolve paths with '..' to normalized paths, raising an error if the final
    result is outside root.
    """
    normalized = posixpath.normpath(api_path.strip('/'))
    if normalized == '.':
        normalized = ''
    elif normalized.startswith('..'):
        raise PathOutsideRoot(normalized)
    return normalized


def outside_root_to_404(fn):
    """
    Decorator for converting PathOutsideRoot errors to 404s.
    """

    @wraps(fn)
    def wrapped(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except PathOutsideRoot as e:
            raise HTTPError(404, "Path outside root: [%s]" % e.args[0])

    return wrapped
