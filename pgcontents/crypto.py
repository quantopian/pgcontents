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
