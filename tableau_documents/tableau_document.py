# -*- coding: utf-8 -*-

from ..tableau_base import TableauBase
from ..tableau_exceptions import *

# This class is just a shell so that TableauWorkbook and TableauDatasource can look the same from TableauFile
# This is probably not that Pythonic but whatevs
class TableauDocument(TableauBase):
    def __init__(self):
        TableauBase.__init__(self)
        self._datasources = []
        self._document_type = None
        self.parameters = None

    @property
    def datasources(self):
        """
        :rtype: list[TableauDatasource]
        """
        return self._datasources

    @property
    def document_type(self):
        """
        :rtype: unicode
        """
        return self._document_type

    def save_file(self, filename_no_extension, save_to_directory=None):
        """
        :type filename_no_extension: unicode
        :type save_to_directory: unicode
        :rtype: bool
        """
        return True


