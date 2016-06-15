"""
Utilities for synchronizing directories.
"""
from __future__ import (
    print_function,
    unicode_literals,
)


from IPython.utils.path import ensure_dir_exists

from ..checkpoints import PostgresCheckpoints
from ..utils.ipycompat import FileContentsManager


def create_user(db_url, user):
    """
    Create a user.
    """
    PostgresCheckpoints(
        db_url=db_url,
        user_id=user,
        create_user_on_startup=True,
    )


def download_checkpoints(db_url, directory, user, crypto):
    """
    Download users' most recent checkpoints to the given directory.
    """
    print("Synchronizing user {user} to {directory}".format(
        user=user, directory=directory,
    ))
    ensure_dir_exists(directory)
    contents_mgr = FileContentsManager(root_dir=directory)
    cp_mgr = PostgresCheckpoints(
        db_url=db_url,
        user_id=user,
        create_user_on_startup=False,
        crypto=crypto,
    )
    cp_mgr.dump(contents_mgr)
    print("Done")


def checkpoint_all(db_url, directory, user):
    """
    Upload the current state of a directory for each user.
    """
    print("Checkpointing directory {directory} for user {user}".format(
        directory=directory, user=user,
    ))

    cp_mgr = PostgresCheckpoints(
        db_url=db_url,
        user_id=user,
        create_user_on_startup=False,
    )
    contents_mgr = FileContentsManager(
        root_dir=directory,
        checkpoints=cp_mgr,
    )
    cps = {}
    for dirname, subdirs, files in walk(contents_mgr):
        for fname in files:
            if fname.endswith('.ipynb'):
                cps[fname] = contents_mgr.create_checkpoint(fname)
    return cps


def _separate_dirs_files(models):
    """
    Split an iterable of models into a list of file paths and a list of
    directory paths.
    """
    dirs = []
    files = []
    for model in models:
        if model['type'] == 'directory':
            dirs.append(model['path'])
        else:
            files.append(model['path'])
    return dirs, files


def walk(mgr):
    """
    Like os.walk, but written in terms of the ContentsAPI.

    Takes a ContentsManager and returns a generator of tuples of the form:
    (directory name, [subdirectories], [files in directory])
    """
    return walk_dirs(mgr, [''])


def walk_dirs(mgr, dirs):
    """
    Recursive helper for walk.
    """
    for directory in dirs:
        children = mgr.get(
            directory,
            content=True,
            type='directory',
        )['content']
        dirs, files = map(sorted, _separate_dirs_files(children))
        yield directory, dirs, files
        if dirs:
            for entry in walk_dirs(mgr, dirs):
                yield entry
