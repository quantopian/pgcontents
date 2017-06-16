"""
Tests for notebook encryption utilities.
"""
from cryptography.fernet import Fernet

from ..crypto import (
    derive_fallback_fernet_keys,
    FallbackCrypto,
    FernetEncryption,
    NoEncryption,
    single_password_crypto_factory,
)


def test_fernet_derivation():
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
    assert encrypted_data_current != data
    assert current_crypto.decrypt(encrypted_data_current) == data

    # Data encrypted with the old key.
    encrypted_data_old = old_crypto.encrypt(data)
    assert encrypted_data_current != data
    assert old_crypto.decrypt(encrypted_data_old) == data

    # The single fernet with the first key should be able to decrypt the
    # multi-fernet's encrypted data.

    assert current_crypto.decrypt(multi_fernet_crypto.encrypt(data)) == data

    # Multi should be able decrypt anything encrypted with either key.
    assert multi_fernet_crypto.decrypt(encrypted_data_current) == data
    assert multi_fernet_crypto.decrypt(encrypted_data_old) == data

    # Unencrypted data should be returned unchanged.
    assert multi_fernet_crypto.decrypt(data) == data
