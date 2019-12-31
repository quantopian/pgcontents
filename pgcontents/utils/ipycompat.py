"""
Utilities for managing compat between notebook versions.
"""
from traitlets.config import Config
from notebook.services.contents.checkpoints import (
    Checkpoints,
    GenericCheckpointsMixin,
)
from notebook.services.contents.filemanager import FileContentsManager
from notebook.services.contents.filecheckpoints import (
    GenericFileCheckpoints
)
from notebook.services.contents.manager import ContentsManager
from notebook.utils import to_os_path
from nbformat import from_dict, reads, writes
from nbformat.v4.nbbase import (
    new_code_cell,
    new_markdown_cell,
    new_notebook,
    new_raw_cell,
)
from nbformat.v4.rwbase import strip_transient
from traitlets import (
    Any,
    Bool,
    Dict,
    Instance,
    Integer,
    HasTraits,
    Unicode,
)


__all__ = [
    'Any',
    'Bool',
    'Checkpoints',
    'Config',
    'ContentsManager',
    'Dict',
    'FileContentsManager',
    'GenericCheckpointsMixin',
    'GenericFileCheckpoints',
    'HasTraits',
    'Instance',
    'Integer',
    'Unicode',
    'from_dict',
    'new_code_cell',
    'new_markdown_cell',
    'new_notebook',
    'new_raw_cell',
    'reads',
    'strip_transient',
    'to_os_path',
    'writes',
]
