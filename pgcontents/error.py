"""
Errors and exceptions for PostgresContentsManager.
"""


class NoSuchDirectory(Exception):
    pass


class NoSuchFile(Exception):
    pass


class FileExists(Exception):
    pass
