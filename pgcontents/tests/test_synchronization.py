"""
Tests for synchronization tools.
"""
from __future__ import unicode_literals
from unittest import TestCase

try:
    from nbformat.v4 import new_markdown_cell
    from nbformat.v4.rwbase import strip_transient
    from notebook.services.contents.filemanager import \
        FileContentsManager
except ImportError:
    from IPython.nbformat.v4 import new_markdown_cell
    from IPython.nbformat.v4.rwbase import strip_transient
    from IPython.html.services.contents.filemanager import \
        FileContentsManager
from IPython.utils.tempdir import TemporaryDirectory
from six import iteritems

from ..checkpoints import PostgresCheckpoints
from .utils import (
    _norm_unicode,
    drop_testing_db_tables,
    migrate_testing_db,
    populate,
    TEST_DB_URL,
)
from ..utils.sync import (
    checkpoint_all,
    download_checkpoints,
)


class TestUploadDownload(TestCase):

    def setUp(self):

        drop_testing_db_tables()
        migrate_testing_db()

        self.td = TemporaryDirectory()
        self.checkpoints = PostgresCheckpoints(
            user_id='test',
            db_url=TEST_DB_URL,
        )
        self.contents = FileContentsManager(
            root_dir=self.td.name,
            checkpoints=self.checkpoints,
        )

        self.checkpoints.ensure_user()

    def tearDown(self):
        self.td.cleanup()

    def add_markdown_cell(self, path):
        # Load and update
        model = self.contents.get(path=path)
        model['content'].cells.append(
            new_markdown_cell('Created by test: ' + path)
        )

        # Save and checkpoint again.
        self.contents.save(model, path=path)
        return model

    def test_download_checkpoints(self):
        """
        Create two checkpoints for two notebooks, then call
        download_checkpoints.

        Assert that we get the correct version of both notebooks.
        """
        self.contents.new({'type': 'directory'}, 'subdir')
        paths = ('a.ipynb', 'subdir/a.ipynb')
        expected_content = {}
        for path in paths:
            # Create and checkpoint.
            self.contents.new(path=path)

            self.contents.create_checkpoint(path)

            model = self.add_markdown_cell(path)
            self.contents.create_checkpoint(path)

            # Assert greater because FileContentsManager creates a checkpoint
            # on creation, but this isn't part of the spec.
            self.assertGreater(len(self.contents.list_checkpoints(path)), 2)

            # Store the content to verify correctness after download.
            expected_content[path] = model['content']

        with TemporaryDirectory() as td:
            download_checkpoints(
                self.checkpoints.db_url,
                td,
                user='test',
            )

            fm = FileContentsManager(root_dir=td)
            root_entries = sorted(m['path'] for m in fm.get('')['content'])
            self.assertEqual(root_entries, ['a.ipynb', 'subdir'])
            subdir_entries = sorted(
                m['path'] for m in fm.get('subdir')['content']
            )
            self.assertEqual(subdir_entries, ['subdir/a.ipynb'])
            for path in paths:
                content = fm.get(path)['content']
                self.assertEqual(expected_content[path], content)

    def test_checkpoint_all(self):
        """
        Test that checkpoint_all correctly makes a checkpoint for all files.
        """
        paths = populate(self.contents)
        original_content_minus_trust = {
            # Remove metadata that we expect to have dropped
            path: strip_transient(self.contents.get(path)['content'])
            for path in paths
        }

        original_cps = {}
        for path in paths:
            # Create a checkpoint, then update the file.
            original_cps[path] = self.contents.create_checkpoint(path)
            self.add_markdown_cell(path)

        # Verify that we still have the old version checkpointed.
        cp_content = {
            path: self.checkpoints.get_notebook_checkpoint(
                cp['id'],
                path,
            )['content']
            for path, cp in iteritems(original_cps)
        }
        self.assertEqual(original_content_minus_trust, cp_content)

        new_cps = checkpoint_all(
            self.checkpoints.db_url,
            self.td.name,
            self.checkpoints.user_id,
        )

        new_cp_content = {
            path: self.checkpoints.get_notebook_checkpoint(
                cp['id'],
                path,
            )['content']
            for path, cp in iteritems(new_cps)
        }
        for path, new_content in iteritems(new_cp_content):
            old_content = original_content_minus_trust[_norm_unicode(path)]
            self.assertEqual(
                new_content['cells'][:-1],
                old_content['cells'],
            )
            self.assertEqual(
                new_content['cells'][-1],
                new_markdown_cell('Created by test: ' + _norm_unicode(path)),
            )
