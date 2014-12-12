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
