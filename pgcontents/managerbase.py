"""
Mixin for classes interacting with the pgcontents database.
"""
from getpass import getuser
from sqlalchemy import (
    create_engine,
)
from sqlalchemy.engine.base import Engine
from tornado.web import HTTPError

from IPython.utils.traitlets import (
    Bool,
    Instance,
    HasTraits,
    Unicode,
)

from .query import ensure_db_user


class PostgresManagerMixin(HasTraits):
    """
    Shared  for Postgres-backed ContentsManagers.
    """

    db_url = Unicode(
        default_value="postgresql://{user}@/pgcontents".format(
            user=getuser(),
        ),
        config=True,
        help="Connection string for the database.",
    )

    user_id = Unicode(
        default_value=getuser(),
        allow_none=True,
        config=True,
        help="Name for the user whose contents we're managing.",
    )

    create_user_on_startup = Bool(
        default_value=True,
        config=True,
        help="Create a user for user_id automatically?",
    )

    engine = Instance(Engine)

    def _engine_default(self):
        return create_engine(self.db_url)

    def __init__(self, *args, **kwargs):
        super(PostgresManagerMixin, self).__init__(*args, **kwargs)
        if self.create_user_on_startup:
            self.ensure_user()

    def ensure_user(self):
        with self.engine.begin() as db:
            ensure_db_user(db, self.user_id)

    def purge_db(self):
        """
        Remove all records associated with our user_id.

        Must be implemented by subclasses.
        """
        raise NotImplementedError()

    def no_such_entity(self, path):
        self.do_404(
            u"No such entity: [{path}]".format(path=path)
        )

    def not_empty(self, path):
        self.do_400(
            u"Directory not empty: [{path}]".format(path=path)
        )

    def file_too_large(self, path):
        self.do_413(u"File is too large to save: [{path}]".format(path=path))

    def already_exists(self, path):
        self.do_409(u"File already exists: [{path}]".format(path=path))

    def do_400(self, msg):
        raise HTTPError(400, msg)

    def do_404(self, msg):
        raise HTTPError(404, msg)

    def do_409(self, msg):
        raise HTTPError(409, msg)

    def do_413(self, msg):
        raise HTTPError(413, msg)

    def do_500(self, msg):
        raise HTTPError(500, msg)
