# encoding: utf-8
"""
Utilities for testing.
"""
from __future__ import unicode_literals
from contextlib import contextmanager

from tornado.web import HTTPError


@contextmanager
def assertRaisesHTTPError(testcase, status, msg=None):
    msg = msg or "Should have raised HTTPError(%i)" % status
    try:
        yield
    except HTTPError as e:
        testcase.assertEqual(e.status_code, status)
    else:
        testcase.fail(msg)
