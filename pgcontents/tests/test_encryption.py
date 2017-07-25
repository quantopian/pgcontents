"""
Tests for notebook encryption utilities.
"""
from unittest import TestCase

from cryptography.fernet import Fernet

from ..crypto import (
    derive_fallback_fernet_keys,
    FallbackCrypto,
    FernetEncryption,
    memoize_single_arg,
    NoEncryption,
    single_password_crypto_factory,
)


class TestEncryption(TestCase):

    def test_fernet_derivation(self):
        pws = [u'currentpassword', u'oldpassword', None]

        # This must be Unicode, so we use the `u` prefix to support py2.
        user_id = u'4e322fa200fffd0001000001'

        current_crypto = single_password_crypto_factory(pws[0])(user_id)
        old_crypto = single_password_crypto_factory(pws[1])(user_id)

        def make_single_key_crypto(key):
            if key is None:
                return NoEncryption()
            return FernetEncryption(Fernet(key.encode('ascii')))

        multi_fernet_crypto = FallbackCrypto(
            [make_single_key_crypto(k)
             for k in derive_fallback_fernet_keys(pws, user_id)]
        )

        data = b'ayy lmao'

        # Data encrypted with the current key.
        encrypted_data_current = current_crypto.encrypt(data)
        self.assertNotEqual(encrypted_data_current, data)
        self.assertEqual(current_crypto.decrypt(encrypted_data_current), data)

        # Data encrypted with the old key.
        encrypted_data_old = old_crypto.encrypt(data)
        self.assertNotEqual(encrypted_data_current, data)
        self.assertEqual(old_crypto.decrypt(encrypted_data_old), data)

        # The single fernet with the first key should be able to decrypt the
        # multi-fernet's encrypted data.
        self.assertEqual(
            current_crypto.decrypt(multi_fernet_crypto.encrypt(data)),
            data
        )

        # Multi should be able decrypt anything encrypted with either key.
        self.assertEqual(multi_fernet_crypto.decrypt(encrypted_data_current),
                         data)
        self.assertEqual(multi_fernet_crypto.decrypt(encrypted_data_old), data)

        # Unencrypted data should be returned unchanged.
        self.assertEqual(multi_fernet_crypto.decrypt(data), data)

    def test_memoize_single_arg(self):
        full_calls = []

        @memoize_single_arg
        def mock_factory(user_id):
            full_calls.append(user_id)
            return u'crypto' + user_id

        calls_to_make = [u'1', u'2', u'3', u'2', u'1']
        expected_results = [u'crypto' + user_id for user_id in calls_to_make]
        expected_full_calls = [u'1', u'2', u'3']

        results = []
        for user_id in calls_to_make:
            results.append(mock_factory(user_id))

        self.assertEqual(results, expected_results)
        self.assertEqual(full_calls, expected_full_calls)
