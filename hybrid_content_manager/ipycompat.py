"""Utilities for managing compat between notebook versions."""

#  if notebook.version_info[0] >= 6:  # noqa
#  raise ImportError("Jupyter Notebook versions 6 and up are not supported.")

from notebook.services.contents.filemanager import FileContentsManager
from notebook.services.contents.manager import ContentsManager
from notebook.services.contents.tests.test_manager import TestContentsManager
from notebook.services.contents.tests.test_contents_api import APITest

from traitlets import Dict

__all__ = [
    'APITest',
    'ContentsManager',
    'Dict',
    'FileContentsManager',
    'TestContentsManager',
]
