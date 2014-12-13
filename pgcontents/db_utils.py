#
# Copyright 2014 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Utilities for working with databases.
"""

from contextlib import contextmanager

from psycopg2.errorcodes import UNIQUE_VIOLATION
from sqlalchemy.exc import IntegrityError


@contextmanager
def ignore_unique_violation():
    try:
        yield
    except IntegrityError as error:
        if error.orig.pgcode != UNIQUE_VIOLATION:
            raise
