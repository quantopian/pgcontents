"""
Tests for synchronization tools.
"""
from __future__ import unicode_literals
from base64 import b64encode
from datetime import datetime
from logging import Logger
from unittest import TestCase

from cryptography.fernet import Fernet
from sqlalchemy import create_engine

from pgcontents import PostgresContentsManager
from pgcontents.api_utils import NBFORMAT_VERSION
from pgcontents.crypto import (
    FernetEncryption,
    NoEncryption,
    single_password_crypto_factory,
)
from pgcontents.query import analyze_files, analyze_checkpoints
from pgcontents.utils.ipycompat import new_markdown_cell, reads

from .utils import (
    assertRaisesHTTPError,
    clear_test_db,
    remigrate_test_schema,
    populate,
    TEST_DB_URL,
)
from ..utils.sync import (
    reencrypt_all_users,
    unencrypt_all_users,
)


class TestReEncryption(TestCase):

    def setUp(self):
        remigrate_test_schema()

    def tearDown(self):
        clear_test_db()

    def add_markdown_cell(self, path):
        # Load and update
        model = self.contents.get(path=path)
        model['content'].cells.append(
            new_markdown_cell('Created by test: ' + path)
        )

        # Save and checkpoint again.
        self.contents.save(model, path=path)
        return model

    def test_reencryption(self):
        """
        Create two unencrypted notebooks and a file, create checkpoints for
        each, then encrypt and check that content is unchanged, then re-encrypt
        and check the same.
        """
        db_url = TEST_DB_URL
        user_id = 'test_reencryption'

        no_crypto = NoEncryption()
        no_crypto_manager = PostgresContentsManager(
            user_id=user_id,
            db_url=db_url,
            crypto=no_crypto,
            create_user_on_startup=True,
        )

        key1 = b'fizzbuzz' * 4
        crypto1 = FernetEncryption(Fernet(b64encode(key1)))
        manager1 = PostgresContentsManager(
            user_id=user_id,
            db_url=db_url,
            crypto=crypto1,
        )

        key2 = key1[::-1]
        crypto2 = FernetEncryption(Fernet(b64encode(key2)))
        manager2 = PostgresContentsManager(
            user_id=user_id,
            db_url=db_url,
            crypto=crypto2,
        )

        # Populate an unencrypted user.
        paths = populate(no_crypto_manager)

        original_content = {}
        for path in paths:
            # Create a checkpoint of the original content and store what we
            # expect it to look like.
            no_crypto_manager.create_checkpoint(path)
            original_content[path] = no_crypto_manager.get(path)['content']

        updated_content = {}
        for path in paths:
            # Create a new version of each notebook with a cell appended.
            model = no_crypto_manager.get(path=path)
            model['content'].cells.append(
                new_markdown_cell('Created by test: ' + path)
            )
            no_crypto_manager.save(model, path=path)

            # Store the updated content.
            updated_content[path] = no_crypto_manager.get(path)['content']

            # Create a checkpoint of the new content.
            no_crypto_manager.create_checkpoint(path)

        def check_path_content(path, mgr, expected):
            retrieved = mgr.get(path)['content']
            self.assertEqual(retrieved, expected[path])

        def check_reencryption(old, new):
            for path in paths:
                # We should no longer be able to retrieve notebooks from the
                # no-crypto manager.
                with assertRaisesHTTPError(self, 500):
                    old.get(path)

                # The new manager should read the latest version of each file.
                check_path_content(path, new, updated_content)

                # We should have two checkpoints available, one from the
                # original version of the file, and one for the updated
                # version.
                (new_cp, old_cp) = new.list_checkpoints(path)
                self.assertGreater(
                    new_cp['last_modified'],
                    old_cp['last_modified'],
                )

                # The old checkpoint should restore us to the original state.
                new.restore_checkpoint(old_cp['id'], path)
                check_path_content(path, new, original_content)

                # The new checkpoint should put us back into our updated state.
                # state.
                new.restore_checkpoint(new_cp['id'], path)
                check_path_content(path, new, updated_content)

        engine = create_engine(db_url)
        logger = Logger('Reencryption Testing')

        no_crypto_factory = {user_id: no_crypto}.__getitem__
        crypto1_factory = {user_id: crypto1}.__getitem__
        crypto2_factory = {user_id: crypto2}.__getitem__

        # Verify that reencryption is idempotent:
        for _ in range(2):
            reencrypt_all_users(
                engine,
                no_crypto_factory,
                crypto1_factory,
                logger,
            )
            check_reencryption(no_crypto_manager, manager1)

        for _ in range(2):
            reencrypt_all_users(
                engine,
                crypto1_factory,
                crypto2_factory,
                logger,
            )
            check_reencryption(manager1, manager2)

        with self.assertRaises(ValueError):
            # Using reencrypt_all_users with a no-encryption target isn't
            # supported.
            reencrypt_all_users(
                engine,
                crypto2_factory,
                no_crypto_factory,
                logger,
            )
        # There should have been no changes from the failed attempt.
        check_reencryption(manager1, manager2)

        # Unencrypt and verify that we can now read everything with the no
        # crypto manager.
        unencrypt_all_users(engine, crypto2_factory, logger)
        check_reencryption(manager2, no_crypto_manager)


class TestAnalyzeNotebooks(TestCase):

    def setUp(self):
        remigrate_test_schema()

    def tearDown(self):
        clear_test_db()

    def test_analyze_notebooks(self):
        """
        Creates files and checkpoints for two users and checks their
        accessibility through `analyze_files` and `analyze_checkpoints`.
        """
        db_url = TEST_DB_URL
        engine = create_engine(db_url)
        user_ids = ['test_analyze_notebooks0', 'test_analyze_notebooks1']
        encryption_pw = u'foobar'

        crypto_factory = single_password_crypto_factory(encryption_pw)
        managers = {}
        for user_id in user_ids:
            managers[user_id] = PostgresContentsManager(
                user_id=user_id,
                db_url=db_url,
                crypto=crypto_factory(user_id),
                create_user_on_startup=True,
            )
        paths = {}

        def create_nbs(user_id):
            """
            Create test notebooks and create a checkpoint for each.
            """
            paths[user_id] = set(populate(managers[user_id]))
            for path in paths[user_id]:
                managers[user_id].create_checkpoint(path)

        # Create test files/remote checkpoints for both users, their creation
        # times separated by a recorded datetime.
        create_nbs(user_ids[0])
        split_dt = datetime.now()
        create_nbs(user_ids[1])

        def process_files_nb(nb):
            """
            For notebook from files table, creates path by concatenating parent
            and filename.
            """
            nb['path'] = nb['parent_name'] + nb['name']

        def check_analyze_call(analyze_fn, expect_users, kwargs={}):
            """
            Calls the given function and checks that all notebooks belonging to
            the specified users are found.
            """
            path_record = {}
            for user_id in expect_users:
                path_record[user_id] = set()
            for result in analyze_fn(engine, crypto_factory, **kwargs):
                if analyze_fn is analyze_files:
                    process_files_nb(result)
                self.assertIn(result['user_id'], expect_users)

                # Converts the content result to a dict format matching the
                # return value of `PostgresContentManager.get()`.
                nb = reads(result['content'], as_version=NBFORMAT_VERSION)
                managers[result['user_id']].mark_trusted_cells(nb,
                                                               result['path'])
                self.assertDictEqual(
                    managers[result['user_id']].get(result['path'])['content'],
                    nb
                )
                path_record[result['user_id']].add(result['path'][1:])

            # Makes sure all notebooks were found.
            for user_id in expect_users:
                self.assertSetEqual(path_record[user_id], paths[user_id])

        def check_analyze_fn(analyze_fn):
            """
            Checks the given function with different kwarg calls.
            """
            max_kwargs = {'max_dt': split_dt}
            min_kwargs = {'min_dt': split_dt}

            check_analyze_call(analyze_fn, user_ids)
            # User 0's notebooks were created before `split_dt`, so they'll be
            # found by the `max_dt` call; the opposite applies to User 1's
            # notebooks.
            check_analyze_call(analyze_fn, [user_ids[0]], max_kwargs)
            check_analyze_call(analyze_fn, [user_ids[1]], min_kwargs)

        check_analyze_fn(analyze_files)
        check_analyze_fn(analyze_checkpoints)
