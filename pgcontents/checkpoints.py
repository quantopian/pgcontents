"""
An IPython FileContentsManager that uses Postgres for checkpoints.
"""
from __future__ import unicode_literals

from .api_utils import (
    _decode_unknown_from_base64,
    outside_root_to_404,
    prefix_dirs,
    reads_base64,
    to_b64,
    writes_base64,
)
from .managerbase import PostgresManagerMixin
from .query import (
    delete_remote_checkpoints,
    delete_single_remote_checkpoint,
    get_remote_checkpoint,
    latest_remote_checkpoints,
    list_remote_checkpoints,
    move_remote_checkpoints,
    purge_remote_checkpoints,
    save_remote_checkpoint,
)
from .utils.ipycompat import Checkpoints, GenericCheckpointsMixin


class PostgresCheckpoints(PostgresManagerMixin,
                          GenericCheckpointsMixin,
                          Checkpoints):
    """
    A Checkpoints implementation that saves checkpoints to a remote database.
    """

    @outside_root_to_404
    def create_notebook_checkpoint(self, nb, path):
        """Create a checkpoint of the current state of a notebook

        Returns a checkpoint_id for the new checkpoint.
        """
        b64_content = writes_base64(nb)
        with self.engine.begin() as db:
            return save_remote_checkpoint(
                db,
                self.user_id,
                path,
                b64_content,
                self.crypto.encrypt,
                self.max_file_size_bytes,
            )

    @outside_root_to_404
    def create_file_checkpoint(self, content, format, path):
        """Create a checkpoint of the current state of a file

        Returns a checkpoint_id for the new checkpoint.
        """
        try:
            b64_content = to_b64(content, format)
        except ValueError as e:
            self.do_400(str(e))
        with self.engine.begin() as db:
            return save_remote_checkpoint(
                db,
                self.user_id,
                path,
                b64_content,
                self.crypto.encrypt,
                self.max_file_size_bytes,
            )

    @outside_root_to_404
    def delete_checkpoint(self, checkpoint_id, path):
        """delete a checkpoint for a file"""
        with self.engine.begin() as db:
            return delete_single_remote_checkpoint(
                db, self.user_id, path, checkpoint_id,
            )

    def _get_checkpoint(self, checkpoint_id, path):
        """Get the content of a checkpoint."""
        with self.engine.begin() as db:
            return get_remote_checkpoint(
                db,
                self.user_id,
                path,
                checkpoint_id,
                self.crypto.decrypt,
            )['content']

    @outside_root_to_404
    def get_notebook_checkpoint(self, checkpoint_id, path):
        b64_content = self._get_checkpoint(checkpoint_id, path)
        return {
            'type': 'notebook',
            'content': reads_base64(b64_content),
        }

    @outside_root_to_404
    def get_file_checkpoint(self, checkpoint_id, path):
        b64_content = self._get_checkpoint(checkpoint_id, path)
        content, format = _decode_unknown_from_base64(path, b64_content)
        return {
            'type': 'file',
            'content': content,
            'format': format,
        }

    @outside_root_to_404
    def list_checkpoints(self, path):
        """Return a list of checkpoints for a given file"""
        with self.engine.begin() as db:
            return list_remote_checkpoints(db, self.user_id, path)

    @outside_root_to_404
    def rename_all_checkpoints(self, old_path, new_path):
        """Rename all checkpoints for old_path to new_path."""
        with self.engine.begin() as db:
            return move_remote_checkpoints(
                db,
                self.user_id,
                old_path,
                new_path,
            )

    @outside_root_to_404
    def delete_all_checkpoints(self, path):
        """Delete all checkpoints for the given path."""
        with self.engine.begin() as db:
            delete_remote_checkpoints(db, self.user_id, path)

    def purge_db(self):
        """
        Purge all database records for the current user.
        """
        with self.engine.begin() as db:
            purge_remote_checkpoints(db, self.user_id)

    def dump(self, contents_mgr):
        """
        Synchronize the state of our database with the specified
        ContentsManager.

        Gets the most recent checkpoint for each file and passes it to the
        supplied ContentsManager to be saved.
        """
        with self.engine.begin() as db:
            records = latest_remote_checkpoints(db, self.user_id)
            for record in records:
                path = record['path']
                if not path.endswith('.ipynb'):
                    self.log.warn('Ignoring non-notebook file: {}', path)
                    continue
                for dirname in prefix_dirs(path):
                    self.log.info("Ensuring directory [%s]" % dirname)
                    contents_mgr.save(
                        model={'type': 'directory'},
                        path=dirname,
                    )
                self.log.info("Writing notebook [%s]" % path)
                contents_mgr.save(
                    self.get_notebook_checkpoint(record['id'], path),
                    path,
                )
