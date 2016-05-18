"""
Errors and exceptions for PostgresContentsManager.
"""


class NoSuchDirectory(Exception):
    pass


class NoSuchFile(Exception):
    pass


class NoSuchCheckpoint(Exception):
    pass


class PathOutsideRoot(Exception):
    pass


class FileExists(Exception):
    pass


class DirectoryExists(Exception):
    pass


class DirectoryNotEmpty(Exception):
    pass


class FileTooLarge(Exception):
    pass


class RenameRoot(Exception):
    pass
