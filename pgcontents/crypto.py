"""
Interface definition for encryption/decryption plugins for
PostgresContentsManager, and implementations of the interface.

Encryption backends should raise pgcontents.error.CorruptedFile if they
encounter an input that they cannot decrypt.
"""
import sys
import base64

from cryptography.fernet import Fernet
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .error import CorruptedFile

if sys.version_info.major == 3:
    unicode = str


class NoEncryption(object):
    """
    No-op encryption backend.

    encrypt() and decrypt() simply return their inputs.

    Methods
    -------
    encrypt : callable[bytes -> bytes]
    decrypt : callable[bytes -> bytes]
    """
    def encrypt(self, b):
        return b

    def decrypt(self, b):
        return b


class FernetEncryption(object):
    """
    Notebook encryption using cryptography.fernet for symmetric-key encryption.

    Parameters
    ----------
    fernet : cryptography.fernet.Fernet
       The Fernet object to use for encryption.

    Methods
    -------
    encrypt : callable[bytes -> bytes]
    decrypt : callable[bytes -> bytes]

    Notes
    -----
    ``cryptography.fernet.MultiFernet`` can be used instead of a vanilla
    ``Fernet`` to allow zero-downtime key rotation.

    See Also
    --------
    :func:`pgcontents.utils.sync.reencrypt_user`
    """
    __slots__ = ('_fernet',)

    def __init__(self, fernet):
        self._fernet = fernet

    def encrypt(self, s):
        return self._fernet.encrypt(s)

    def decrypt(self, s):
        try:
            return self._fernet.decrypt(s)
        except Exception as e:
            raise CorruptedFile(e)

    def __copy__(self, memo):
        # Any value that appears in an IPython/Jupyter Config object needs to
        # be deepcopy-able. Cryptography's Fernet objects aren't deepcopy-able,
        # so we copy our underlying state to a new FernetEncryption object.
        return FernetEncryption(self._fernet)

    def __deepcopy__(self, memo):
        # Any value that appears in an IPython/Jupyter Config object needs to
        # be deepcopy-able. Cryptography's Fernet objects aren't deepcopy-able,
        # so we copy our underlying state to a new FernetEncryption object.
        return FernetEncryption(self._fernet)


class FallbackCrypto(object):
    """
    Notebook encryption that accepts a list of crypto instances and decrypts by
    trying them in order.

    Sub-cryptos should raise ``CorruptedFile`` if they're unable to decrypt an
    input.

    This is conceptually similar to the technique used by
    ``cryptography.fernet.MultiFernet`` for implementing key rotation.

    Parameters
    ----------
    cryptos : list[object]
       A sequence of cryptos to use for decryption. cryptos[0] will always be
       used for encryption.

    Methods
    -------
    encrypt : callable[bytes -> bytes]
    decrypt : callable[bytes -> bytes]

    Notes
    -----
    Since NoEncryption will always succeed, it is only supported as the last
    entry in ``cryptos``.  Passing a list with a NoEncryption not in the last
    location will raise a ValueError.
    """
    __slots__ = ('_cryptos',)

    def __init__(self, cryptos):
        # Only the last crypto can be a ``NoEncryption``.
        for c in cryptos[:-1]:
            if isinstance(c, NoEncryption):
                raise ValueError(
                    "NoEncryption is only supported as the last fallback."
                )

        self._cryptos = cryptos

    def encrypt(self, s):
        return self._cryptos[0].encrypt(s)

    def decrypt(self, s):
        errors = []
        for c in self._cryptos:
            try:
                return c.decrypt(s)
            except CorruptedFile as e:
                errors.append(e)
        raise CorruptedFile(errors)


def ascii_unicode_to_bytes(v):
    assert isinstance(v, unicode), "Expected unicode, got %s" % type(v)
    return v.encode('ascii')


def derive_single_fernet_key(password, user_id):
    """
    Convert a secret key and a user ID into an encryption key to use with a
    ``cryptography.fernet.Fernet``.

    Taken from
    https://cryptography.io/en/latest/fernet/#using-passwords-with-fernet

    Parameters
    ----------
    password : unicode
        List of ascii-encodable keys to derive.
    user_id : unicode
        ascii-encodable user_id to use as salt
    """
    password = ascii_unicode_to_bytes(password)
    user_id = ascii_unicode_to_bytes(user_id)

    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=user_id,
        iterations=100000,
        backend=default_backend(),
    )
    return base64.urlsafe_b64encode(kdf.derive(password))


def derive_fallback_fernet_keys(passwords, user_id):
    """
    Derive a list of per-user Fernet keys from a list of master keys and a
    username.

    If a None is encountered in ``passwords``, it is forwarded.

    Parameters
    ----------
    passwords : list[unicode]
        List of ascii-encodable keys to derive.
    user_id : unicode or None
        ascii-encodable user_id to use as salt
    """
    # Normally I wouldn't advocate for these kinds of assertions, but we really
    # really really don't want to mess up deriving encryption keys.
    assert isinstance(passwords, (list, tuple)), \
        "Expected list or tuple of keys, got %s." % type(passwords)

    def derive_single_allow_none(k):
        if k is None:
            return None
        return derive_single_fernet_key(k, user_id).decode('ascii')

    return list(map(derive_single_allow_none, passwords))


def no_password_crypto_factory():
    """
    Create and return a function suitable for passing as a crypto_factory to
    ``pgcontents.utils.sync.reencrypt_all_users``

    The factory here always returns NoEncryption().  This is useful when passed
    as ``old_crypto_factory`` to a database that hasn't yet been encrypted.
    """
    def factory(user_id):
        return NoEncryption()
    return factory


def single_password_crypto_factory(password):
    """
    Create and return a function suitable for passing as a crypto_factory to
    ``pgcontents.utils.sync.reencrypt_all_users``

    The factory here returns a ``FernetEncryption`` that uses a key derived
    from ``password`` and salted with the supplied user_id.
    """
    def factory(user_id):
        return FernetEncryption(
            Fernet(derive_single_fernet_key(password, user_id))
        )
    return factory
