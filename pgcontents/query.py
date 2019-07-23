"""
Database Queries for PostgresContentsManager.
"""
from sqlalchemy import (
    and_,
    cast,
    desc,
    func,
    null,
    select,
    Unicode,
)

from sqlalchemy.exc import IntegrityError

from .api_utils import (
    from_api_dirname,
    from_api_filename,
    reads_base64,
    split_api_filepath,
    to_api_path,
)
from .constants import UNLIMITED
from .db_utils import (
    ignore_unique_violation,
    is_unique_violation,
    is_foreign_key_violation,
    to_dict_no_content,
    to_dict_with_content,
)
from .error import (
    CorruptedFile,
    DirectoryNotEmpty,
    FileExists,
    DirectoryExists,
    FileTooLarge,
    NoSuchCheckpoint,
    NoSuchDirectory,
    NoSuchFile,
    RenameRoot,
)
from .schema import (
    directories,
    files,
    remote_checkpoints,
    users,
)

# ===============================
# Encryption/Decryption Utilities
# ===============================


def preprocess_incoming_content(content, encrypt_func, max_size_bytes):
    """
    Apply preprocessing steps to file/notebook content that we're going to
    write to the database.

    Applies ``encrypt_func`` to ``content`` and checks that the result is
    smaller than ``max_size_bytes``.
    """
    encrypted = encrypt_func(content)
    if max_size_bytes != UNLIMITED and len(encrypted) > max_size_bytes:
        raise FileTooLarge()
    return encrypted


def unused_decrypt_func(s):
    """
    Used by invocations of ``get_file`` that don't expect decrypt_func to be
    called.
    """
    raise AssertionError("Unexpected decrypt call.")


# =====
# Users
# =====

def list_users(db):
    return db.execute(select([users.c.id]))


def ensure_db_user(db, user_id):
    """
    Add a new user if they don't already exist.
    """
    with ignore_unique_violation():
        db.execute(
            users.insert().values(id=user_id),
        )


def purge_user(db, user_id):
    """
    Delete a user and all of their resources.
    """
    db.execute(files.delete().where(
        files.c.user_id == user_id
    ))
    db.execute(directories.delete().where(
        directories.c.user_id == user_id
    ))
    db.execute(users.delete().where(
        users.c.id == user_id
    ))


# ===========
# Directories
# ===========
def create_directory(db, user_id, api_path):
    """
    Create a directory.
    """
    name = from_api_dirname(api_path)
    if name == '/':
        parent_name = null()
        parent_user_id = null()
    else:
        # Convert '/foo/bar/buzz/' -> '/foo/bar/'
        parent_name = name[:name.rindex('/', 0, -1) + 1]
        parent_user_id = user_id

    db.execute(
        directories.insert().values(
            name=name,
            user_id=user_id,
            parent_name=parent_name,
            parent_user_id=parent_user_id,
        )
    )


def ensure_directory(db, user_id, api_path):
    """
    Ensure that the given user has the given directory.
    """
    with ignore_unique_violation():
        create_directory(db, user_id, api_path)


def _is_in_directory(table, user_id, db_dirname):
    """
    Return a WHERE clause that matches entries in a directory.

    Parameterized on table because this clause is re-used between files and
    directories.
    """
    return and_(
        table.c.parent_name == db_dirname,
        table.c.user_id == user_id,
    )


def _directory_default_fields():
    """
    Default fields returned by a directory query.
    """
    return [
        directories.c.name,
    ]


def delete_directory(db, user_id, api_path):
    """
    Delete a directory.
    """
    db_dirname = from_api_dirname(api_path)
    try:
        result = db.execute(
            directories.delete().where(
                and_(
                    directories.c.user_id == user_id,
                    directories.c.name == db_dirname,
                )
            )
        )
    except IntegrityError as error:
        if is_foreign_key_violation(error):
            raise DirectoryNotEmpty(api_path)
        else:
            raise

    rowcount = result.rowcount
    if not rowcount:
        raise NoSuchDirectory(api_path)

    return rowcount


def dir_exists(db, user_id, api_dirname):
    """
    Check if a directory exists.
    """
    return _dir_exists(db, user_id, from_api_dirname(api_dirname))


def _dir_exists(db, user_id, db_dirname):
    """
    Internal implementation of dir_exists.

    Expects a db-style path name.
    """
    return db.execute(
        select(
            [func.count(directories.c.name)],
        ).where(
            and_(
                directories.c.user_id == user_id,
                directories.c.name == db_dirname,
            ),
        )
    ).scalar() != 0


def files_in_directory(db, user_id, db_dirname):
    """
    Return files in a directory.
    """
    fields = _file_default_fields()
    rows = db.execute(
        select(
            fields,
        ).where(
            _is_in_directory(files, user_id, db_dirname),
        ).order_by(
            files.c.user_id,
            files.c.parent_name,
            files.c.name,
            files.c.created_at,
        ).distinct(
            files.c.user_id, files.c.parent_name, files.c.name,
        )
    )
    return [to_dict_no_content(fields, row) for row in rows]


def directories_in_directory(db, user_id, db_dirname):
    """
    Return subdirectories of a directory.
    """
    fields = _directory_default_fields()
    rows = db.execute(
        select(
            fields,
        ).where(
            _is_in_directory(directories, user_id, db_dirname),
        )
    )
    return [to_dict_no_content(fields, row) for row in rows]


def get_directory(db, user_id, api_dirname, content):
    """
    Return the names of all files/directories that are direct children of
    api_dirname.

    If content is False, return a bare model containing just a database-style
    name.
    """
    db_dirname = from_api_dirname(api_dirname)
    if not _dir_exists(db, user_id, db_dirname):
        raise NoSuchDirectory(api_dirname)
    if content:
        files = files_in_directory(
            db,
            user_id,
            db_dirname,
        )
        subdirectories = directories_in_directory(
            db,
            user_id,
            db_dirname,
        )
    else:
        files, subdirectories = None, None

    # TODO: Consider using namedtuples for these return values.
    return {
        'name': db_dirname,
        'files': files,
        'subdirs': subdirectories,
    }


# =====
# Files
# =====
def _file_where(user_id, api_path):
    """
    Return a WHERE clause matching the given API path and user_id.
    """
    directory, name = split_api_filepath(api_path)
    return and_(
        files.c.name == name,
        files.c.user_id == user_id,
        files.c.parent_name == directory,
    )


def _file_creation_order():
    """
    Return an order_by on file creation date.
    """
    return desc(files.c.created_at)


def _select_file(user_id, api_path, fields, limit):
    """
    Return a SELECT statement that returns the latest N versions of a file.
    """
    query = select(fields).where(
        _file_where(user_id, api_path),
    ).order_by(
        _file_creation_order(),
    )
    if limit is not None:
        query = query.limit(limit)

    return query


def _file_default_fields():
    """
    Default fields returned by a file query.
    """
    return [
        files.c.name,
        files.c.created_at,
        files.c.parent_name,
    ]


def _get_file(db, user_id, api_path, query_fields, decrypt_func):
    """
    Get file data for the given user_id, path, and query_fields.  The
    query_fields parameter specifies which database fields should be
    included in the returned file data.
    """
    result = db.execute(
        _select_file(user_id, api_path, query_fields, limit=1),
    ).first()

    if result is None:
        raise NoSuchFile(api_path)

    if files.c.content in query_fields:
        return to_dict_with_content(query_fields, result, decrypt_func)
    else:
        return to_dict_no_content(query_fields, result)


def get_file(db, user_id, api_path, include_content, decrypt_func):
    """
    Get file data for the given user_id and path.

    Include content only if include_content=True.
    """
    query_fields = _file_default_fields()
    if include_content:
        query_fields.append(files.c.content)

    return _get_file(db, user_id, api_path, query_fields, decrypt_func)


def get_file_id(db, user_id, api_path):
    """
    Get the value in the 'id' column for the file with the given
    user_id and path.
    """
    return _get_file(
        db,
        user_id,
        api_path,
        [files.c.id],
        unused_decrypt_func,
    )['id']


def delete_file(db, user_id, api_path):
    """
    Delete a file.

    TODO: Consider making this a soft delete.
    """
    result = db.execute(
        files.delete().where(
            _file_where(user_id, api_path)
        )
    )

    rowcount = result.rowcount
    if not rowcount:
        raise NoSuchFile(api_path)

    return rowcount


def file_exists(db, user_id, path):
    """
    Check if a file exists.
    """
    try:
        get_file(
            db,
            user_id,
            path,
            include_content=False,
            decrypt_func=unused_decrypt_func,
        )
        return True
    except NoSuchFile:
        return False


def rename_file(db, user_id, old_api_path, new_api_path):
    """
    Rename a file.
    """
    # Overwriting existing files is disallowed.
    if file_exists(db, user_id, new_api_path):
        raise FileExists(new_api_path)

    new_dir, new_name = split_api_filepath(new_api_path)

    db.execute(
        files.update().where(
            _file_where(user_id, old_api_path),
        ).values(
            name=new_name,
            parent_name=new_dir,
            created_at=func.now(),
        )
    )


def rename_directory(db, user_id, old_api_path, new_api_path):
    """
    Rename a directory.
    """
    old_db_path = from_api_dirname(old_api_path)
    new_db_path = from_api_dirname(new_api_path)

    if old_db_path == '/':
        raise RenameRoot('Renaming the root directory is not permitted.')

    # Overwriting existing directories is disallowed.
    if _dir_exists(db, user_id, new_db_path):
        raise DirectoryExists(new_api_path)

    # Set this foreign key constraint to deferred so it's not violated
    # when we run the first statement to update the name of the directory.
    db.execute('SET CONSTRAINTS '
               'pgcontents.directories_parent_user_id_fkey DEFERRED')

    new_api_dir, new_name = split_api_filepath(new_api_path)
    new_db_dir = from_api_dirname(new_api_dir)

    # Update the name and parent_name columns for the directory that is being
    # renamed. The parent_name column will not change for a simple rename, but
    # will if the directory is moving.
    db.execute(
        directories.update().where(
            and_(
                directories.c.user_id == user_id,
                directories.c.name == old_db_path,
            )
        ).values(
            name=new_db_path,
            parent_name=new_db_dir,
        )
    )

    # Update the name and parent_name of any descendant directories. Do this in
    # a single statement so the non-deferrable check constraint is satisfied.
    db.execute(
        directories.update().where(
            and_(
                directories.c.user_id == user_id,
                directories.c.name.startswith(old_db_path),
                directories.c.parent_name.startswith(old_db_path),
            ),
        ).values(
            name=func.concat(
                new_db_path,
                func.right(directories.c.name, -func.length(old_db_path)),
            ),
            parent_name=func.concat(
                new_db_path,
                func.right(
                    directories.c.parent_name,
                    -func.length(old_db_path),
                ),
            ),
        )
    )


def save_file(db, user_id, path, content, encrypt_func, max_size_bytes):
    """
    Save a file.

    TODO: Update-then-insert is probably cheaper than insert-then-update.
    """
    content = preprocess_incoming_content(
        content,
        encrypt_func,
        max_size_bytes,
    )
    directory, name = split_api_filepath(path)
    with db.begin_nested() as savepoint:
        try:
            res = db.execute(
                files.insert().values(
                    name=name,
                    user_id=user_id,
                    parent_name=directory,
                    content=content,
                )
            )
        except IntegrityError as error:
            # The file already exists, so overwrite its content with the newer
            # version.
            if is_unique_violation(error):
                savepoint.rollback()
                res = db.execute(
                    files.update().where(
                        _file_where(user_id, path),
                    ).values(
                        content=content,
                        created_at=func.now(),
                    )
                )
            else:
                # Unknown error.  Reraise
                raise

    return res


def generate_files(engine, crypto_factory, min_dt=None, max_dt=None,
                   logger=None):
    """
    Create a generator of decrypted files.

    Files are yielded in ascending order of their timestamp.

    This function selects all current notebooks (optionally, falling within a
    datetime range), decrypts them, and returns a generator yielding dicts,
    each containing a decoded notebook and metadata including the user,
    filepath, and timestamp.

    Parameters
    ----------
    engine : SQLAlchemy.engine
        Engine encapsulating database connections.
    crypto_factory : function[str -> Any]
        A function from user_id to an object providing the interface required
        by PostgresContentsManager.crypto.  Results of this will be used for
        decryption of the selected notebooks.
    min_dt : datetime.datetime, optional
        Minimum last modified datetime at which a file will be included.
    max_dt : datetime.datetime, optional
        Last modified datetime at and after which a file will be excluded.
    logger : Logger, optional
    """
    return _generate_notebooks(files, files.c.created_at,
                               engine, crypto_factory, min_dt, max_dt, logger)


# =======================================
# Checkpoints (PostgresCheckpoints)
# =======================================
def _remote_checkpoint_default_fields():
    return [
        cast(remote_checkpoints.c.id, Unicode),
        remote_checkpoints.c.last_modified,
    ]


def delete_single_remote_checkpoint(db, user_id, api_path, checkpoint_id):
    db_path = from_api_filename(api_path)
    result = db.execute(
        remote_checkpoints.delete().where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path == db_path,
                remote_checkpoints.c.id == int(checkpoint_id),
            ),
        ),
    )

    if not result.rowcount:
        raise NoSuchCheckpoint(api_path, checkpoint_id)


def delete_remote_checkpoints(db, user_id, api_path):
    db_path = from_api_filename(api_path)
    db.execute(
        remote_checkpoints.delete().where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path == db_path,
            ),
        )
    )


def list_remote_checkpoints(db, user_id, api_path):
    db_path = from_api_filename(api_path)
    fields = _remote_checkpoint_default_fields()
    results = db.execute(
        select(fields).where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path == db_path,
            ),
        ).order_by(
            desc(remote_checkpoints.c.last_modified),
        ),
    )

    return [to_dict_no_content(fields, row) for row in results]


def move_single_remote_checkpoint(db,
                                  user_id,
                                  src_api_path,
                                  dest_api_path,
                                  checkpoint_id):
    src_db_path = from_api_filename(src_api_path)
    dest_db_path = from_api_filename(dest_api_path)
    result = db.execute(
        remote_checkpoints.update().where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path == src_db_path,
                remote_checkpoints.c.id == int(checkpoint_id),
            ),
        ).values(
            path=dest_db_path,
        ),
    )

    if not result.rowcount:
        raise NoSuchCheckpoint(src_api_path, checkpoint_id)


def move_remote_checkpoints(db, user_id, src_api_path, dest_api_path):
    src_db_path = from_api_filename(src_api_path)
    dest_db_path = from_api_filename(dest_api_path)

    # Update the paths of the checkpoints for the file being renamed. If the
    # source path is for a directory then this is a no-op.
    db.execute(
        remote_checkpoints.update().where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path == src_db_path,
            ),
        ).values(
            path=dest_db_path,
        ),
    )

    # If the given source path is for a directory, update the paths of the
    # checkpoints for all files in that directory and its subdirectories.
    db.execute(
        remote_checkpoints.update().where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path.startswith(src_db_path),
            ),
        ).values(
            path=func.concat(
                dest_db_path,
                func.right(
                    remote_checkpoints.c.path,
                    -func.length(src_db_path),
                ),
            ),
        )
    )


def get_remote_checkpoint(db, user_id, api_path, checkpoint_id, decrypt_func):
    db_path = from_api_filename(api_path)
    fields = [remote_checkpoints.c.content]
    result = db.execute(
        select(
            fields,
        ).where(
            and_(
                remote_checkpoints.c.user_id == user_id,
                remote_checkpoints.c.path == db_path,
                remote_checkpoints.c.id == int(checkpoint_id),
            ),
        )
    ).first()  # NOTE: This applies a LIMIT 1 to the query.

    if result is None:
        raise NoSuchCheckpoint(api_path, checkpoint_id)

    return to_dict_with_content(fields, result, decrypt_func)


def save_remote_checkpoint(db,
                           user_id,
                           api_path,
                           content,
                           encrypt_func,
                           max_size_bytes):
    # IMPORTANT NOTE: Read the long comment at the top of
    # ``reencrypt_user_content`` before you change this function.

    content = preprocess_incoming_content(
        content,
        encrypt_func,
        max_size_bytes,
    )
    return_fields = _remote_checkpoint_default_fields()
    result = db.execute(
        remote_checkpoints.insert().values(
            user_id=user_id,
            path=from_api_filename(api_path),
            content=content,
        ).returning(
            *return_fields
        ),
    ).first()

    return to_dict_no_content(return_fields, result)


def purge_remote_checkpoints(db, user_id):
    """
    Delete all database records for the given user_id.
    """
    db.execute(
        remote_checkpoints.delete().where(
            remote_checkpoints.c.user_id == user_id,
        )
    )


def generate_checkpoints(engine, crypto_factory, min_dt=None, max_dt=None,
                         logger=None):
    """
    Create a generator of decrypted remote checkpoints.

    Checkpoints are yielded in ascending order of their timestamp.

    This function selects all notebook checkpoints (optionally, falling within
    a datetime range), decrypts them, and returns a generator yielding dicts,
    each containing a decoded notebook and metadata including the user,
    filepath, and timestamp.

    Parameters
    ----------
    engine : SQLAlchemy.engine
        Engine encapsulating database connections.
    crypto_factory : function[str -> Any]
        A function from user_id to an object providing the interface required
        by PostgresContentsManager.crypto.  Results of this will be used for
        decryption of the selected notebooks.
    min_dt : datetime.datetime, optional
        Minimum last modified datetime at which a file will be included.
    max_dt : datetime.datetime, optional
        Last modified datetime at and after which a file will be excluded.
    logger : Logger, optional
    """
    return _generate_notebooks(remote_checkpoints,
                               remote_checkpoints.c.last_modified,
                               engine, crypto_factory, min_dt, max_dt, logger)


# ====================
# Files or Checkpoints
# ====================
def _generate_notebooks(table, timestamp_column,
                        engine, crypto_factory, min_dt, max_dt, logger):
    """
    See docstrings for `generate_files` and `generate_checkpoints`.

    Parameters
    ----------
    table : SQLAlchemy.Table
        Table to fetch notebooks from, `files` or `remote_checkpoints.
    timestamp_column : SQLAlchemy.Column
        `table`'s column storing timestamps, `created_at` or `last_modified`.
    engine : SQLAlchemy.engine
        Engine encapsulating database connections.
    crypto_factory : function[str -> Any]
        A function from user_id to an object providing the interface required
        by PostgresContentsManager.crypto.  Results of this will be used for
        decryption of the selected notebooks.
    min_dt : datetime.datetime
        Minimum last modified datetime at which a file will be included.
    max_dt : datetime.datetime
        Last modified datetime at and after which a file will be excluded.
    logger : Logger
    """
    where_conds = []
    if min_dt is not None:
        where_conds.append(timestamp_column >= min_dt)
    if max_dt is not None:
        where_conds.append(timestamp_column < max_dt)
    if table is files:
        # Only select files that are notebooks
        where_conds.append(files.c.name.like(u'%.ipynb'))

    # Query for notebooks satisfying the conditions.
    query = select([table]).order_by(timestamp_column)
    for cond in where_conds:
        query = query.where(cond)
    result = engine.execute(query)

    # Decrypt each notebook and yield the result.
    for nb_row in result:
        try:
            # The decrypt function depends on the user
            user_id = nb_row['user_id']
            decrypt_func = crypto_factory(user_id).decrypt

            nb_dict = to_dict_with_content(table.c, nb_row, decrypt_func)
            if table is files:
                # Correct for files schema differing somewhat from checkpoints.
                nb_dict['path'] = nb_dict['parent_name'] + nb_dict['name']
                nb_dict['last_modified'] = nb_dict['created_at']

            # For 'content', we use `reads_base64` directly. If the db content
            # format is changed from base64, the decoding should be changed
            # here as well.
            yield {
                'id': nb_dict['id'],
                'user_id': user_id,
                'path': to_api_path(nb_dict['path']),
                'last_modified': nb_dict['last_modified'],
                'content': reads_base64(nb_dict['content']),
            }
        except CorruptedFile:
            if logger is not None:
                logger.warning(
                    'Corrupted file with id %d in table %s.'
                    % (nb_row['id'], table.name)
                )


##########################
# Reencryption Utilities #
##########################
def reencrypt_row_content(db,
                          table,
                          row_id,
                          decrypt_func,
                          encrypt_func,
                          logger):
    """
    Re-encrypt a row from ``table`` with ``id`` of ``row_id``.
    """
    q = (select([table.c.content])
         .with_for_update()
         .where(table.c.id == row_id))

    [(content,)] = db.execute(q)

    logger.info("Begin encrypting %s row %s.", table.name, row_id)
    db.execute(
        table
        .update()
        .where(table.c.id == row_id)
        .values(content=encrypt_func(decrypt_func(content)))
    )
    logger.info("Done encrypting %s row %s.", table.name, row_id)


def select_file_ids(db, user_id):
    """
    Get all file ids for a user.
    """
    return list(
        db.execute(
            select([files.c.id])
            .where(files.c.user_id == user_id)
        )
    )


def select_remote_checkpoint_ids(db, user_id):
    """
    Get all file ids for a user.
    """
    return list(
        db.execute(
            select([remote_checkpoints.c.id])
            .where(remote_checkpoints.c.user_id == user_id)
        )
    )


def reencrypt_user_content(engine,
                           user_id,
                           old_decrypt_func,
                           new_encrypt_func,
                           logger):
    """
    Re-encrypt all of the files and checkpoints for a single user.
    """
    logger.info("Begin re-encryption for user %s", user_id)
    with engine.begin() as db:
        # NOTE: Doing both of these operations in one transaction depends for
        # correctness on the fact that the creation of new checkpoints always
        # involves writing new data into the database from Python, rather than
        # simply copying data inside the DB.

        # If we change checkpoint creation so that it does an in-database copy,
        # then we need to split this transaction to ensure that
        # file-reencryption is complete before checkpoint-reencryption starts.

        # If that doesn't happen, it will be possible for a user to create a
        # new checkpoint in a transaction that hasn't seen the completed
        # file-reencryption process, but we might not see that checkpoint here,
        # which means that we would never update the content of that checkpoint
        # to the new encryption key.
        logger.info("Re-encrypting files for %s", user_id)
        for (file_id,) in select_file_ids(db, user_id):
            reencrypt_row_content(
                db,
                files,
                file_id,
                old_decrypt_func,
                new_encrypt_func,
                logger,
            )

        logger.info("Re-encrypting checkpoints for %s", user_id)
        for (cp_id,) in select_remote_checkpoint_ids(db, user_id):
            reencrypt_row_content(
                db,
                remote_checkpoints,
                cp_id,
                old_decrypt_func,
                new_encrypt_func,
                logger,
            )
    logger.info("Finished re-encryption for user %s", user_id)
