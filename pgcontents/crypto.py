"""
Interface definition for encryption/decryption plugins for
PostgresContentsManager.

Encryption backends should raise pgcontents.error.CorruptedFile if they
encounter an input that they cannot decrypt.
"""
from .error import CorruptedFile


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
