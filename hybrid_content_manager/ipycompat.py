"""Utilities for managing compat between notebook versions."""

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
