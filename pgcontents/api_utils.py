"""
Utilities for implementing the ContentsManager API.
"""
from __future__ import unicode_literals
from base64 import (
    b64decode,
    b64encode,
)
from datetime import datetime
from functools import wraps
import mimetypes
import posixpath

try:
    from nbformat import (
        reads,
        writes,
    )
except ImportError:
    from IPython.nbformat import (
        reads,
        writes,
    )
from tornado.web import HTTPError
from .error import PathOutsideRoot

NBFORMAT_VERSION = 4

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


def api_path_join(*paths):
    """
    Join API-style paths.
    """
    return posixpath.join(*paths).strip('/')


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


def from_api_dirname(api_dirname):
    """
    Convert API-style directory name into a db-style directory name.
    """
    normalized = normalize_api_path(api_dirname)
    if normalized == '':
        return '/'
    return '/' + normalized + '/'


def from_api_filename(api_path):
    """
    Convert an API-style path into a db-style path.
    """
    normalized = normalize_api_path(api_path)
    assert len(normalized), "Empty path in from_api_filename"
    return '/' + normalized


def to_api_path(db_path):
    """
    Convert database path into API-style path.
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


def writes_base64(nb, version=NBFORMAT_VERSION):
    """
    Write a notebook as base64.
    """
    return b64encode(writes(nb, version=version).encode('utf-8'))


def reads_base64(nb, as_version=NBFORMAT_VERSION):
    """
    Read a notebook from base64.
    """
    return reads(b64decode(nb).decode('utf-8'), as_version=as_version)


def _decode_text_from_base64(path, bcontent):
    content = b64decode(bcontent)
    try:
        return (content.decode('utf-8'), 'text')
    except UnicodeError:
        raise HTTPError(
            400,
            "%s is not UTF-8 encoded" % path, reason='bad format'
        )


def _decode_unknown_from_base64(path, bcontent):
    """
    Decode base64 data of unknown format.

    Attempts to interpret data as utf-8, falling back to ascii on failure.
    """
    content = b64decode(bcontent)
    try:
        return (content.decode('utf-8'), 'text')
    except UnicodeError:
        pass
    return bcontent.decode('ascii'), 'base64'


def from_b64(path, bcontent, format):
    """
    Decode base64 content for a file.

    format:
      If 'text', the contents will be decoded as UTF-8.
      If 'base64', do nothing.
      If not specified, try to decode as UTF-8, and fall back to base64

    Returns a triple of decoded_content, format, and mimetype.
    """
    decoders = {
        'base64': lambda path, bcontent: (bcontent.decode('ascii'), 'base64'),
        'text': _decode_text_from_base64,
        None: _decode_unknown_from_base64,
    }
    content, real_format = decoders[format](path, bcontent)

    default_mimes = {
        'text': 'text/plain',
        'base64': 'application/octet-stream',
    }
    mimetype = mimetypes.guess_type(path)[0] or default_mimes[real_format]

    return content, real_format, mimetype


def to_b64(content, fmt):
    allowed_formats = {'text', 'base64'}
    if fmt not in allowed_formats:
        raise ValueError(
            "Expected file contents in {allowed}, got {fmt}".format(
                allowed=allowed_formats,
                fmt=fmt,
            )
        )
    if fmt == 'text':
        # Unicode -> bytes -> base64-encoded bytes.
        return b64encode(content.encode('utf8'))
    else:
        return content.encode('ascii')


def prefix_dirs(path):
    """
    Return an iterable of all prefix directories of path, descending from root.
    """
    _dirname = posixpath.dirname
    path = path.strip('/')
    out = []
    while path != '':
        path = _dirname(path)
        out.append(path)
    return reversed(out)


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
