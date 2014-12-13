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
Tests for PostgresContentsManager.
"""


from unittest import TestCase

from IPython.nbformat import v4 as nbformat
from IPython.html.services.contents.tests.test_manager import TestFileContentsManager

from ..pgmanager import PostgresContentsManager

from unittest import TestCase


# class PostgresContentsManagerTestCase(TestCase):

#     def setUp(self):
#         self.contents_manager = PostgresContentsManager()

#     def tearDown(self):
#         pass

#     def add_code_cell(self, nb):
#         output = nbformat.new_output(
#             "display_data",
#             {'application/javascript': "alert('hi');"},
#         )
#         cell = nbformat.new_code_cell("print('hi')", outputs=[output])
#         nb.cells.append(cell)

#     def new_notebook(self):
#         cm = self.contents_manager
#         model = cm.new_untitled(type='notebook')
#         name = model['name']
#         path = model['path']

#         full_model = cm.get(path)
#         nb = full_model['content']
#         self.add_code_cell(nb)

#         cm.save(full_model, path)
#         return nb, name, path

#     def test_nothing(self):
#         pass
