"""
Utilities for synchronizing directories.
"""
from __future__ import (
    print_function,
    unicode_literals,
)

from ..checkpoints import PostgresCheckpoints
from ..query import (
    list_users,
    reencrypt_user_content,
)


def create_user(db_url, user):
    """
    Create a user.
    """
    PostgresCheckpoints(
        db_url=db_url,
        user_id=user,
        create_user_on_startup=True,
    )


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


def walk_files(mgr):
    """
    Iterate over all files visible to ``mgr``.
    """
    for dir_, subdirs, files in walk_files(mgr):
        for file_ in files:
            yield file_


def all_user_ids(engine):
    """
    Get a list of user_ids from an engine.
    """
    with engine.begin() as db:
        return [row[0] for row in list_users(db)]


def reencrypt_all_users(engine,
                        old_crypto_factory,
                        new_crypto_factory,
                        logger):
    """
    Re-encrypt data for all users.

    Parameters
    ----------
    engine : SQLAlchemy.engine
        Engine encapsulating database connections.
    old_crypto_factory : function[str -> Any]
        A function from user_id to an object providing the interface required
        by PostgresContentsManager.crypto.  Results of this will be used for
        decryption of existing database content.
    new_crypto_factory : function[str -> Any]
        A function from user_id to an object providing the interface required
        by PostgresContentsManager.crypto.  Results of this will be used for
        re-encryption of database content.
    logger : logging.Logger, optional
        A logger to user during re-encryption.
    """
    logger.info("Beginning re-encryption for all users.")
    for user_id in all_user_ids(engine):
        reencrypt_user(
            engine,
            user_id,
            old_crypto=old_crypto_factory(user_id),
            new_crypto=new_crypto_factory(user_id),
            logger=logger,
        )
    logger.info("Finished re-encryption for all users.")


def reencrypt_user(engine, user_id, old_crypto, new_crypto, logger):
    """
    Re-encrypt all files and checkpoints for a single user.
    """
    reencrypt_user_content(
        engine,
        user_id,
        old_crypto.decrypt,
        new_crypto.encrypt,
        logger=logger,
    )
