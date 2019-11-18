# -*- coding: utf-8 -*-
from typing import Union, Any, Optional, List, Dict, Tuple

from tableau_tools.tableau_base import TableauBase
from tableau_tools.tableau_exceptions import *
# from tableau_documents.tableau_datasource import TableauDatasource

# This class is just a shell so that TableauWorkbook and TableauDatasource can look the same from TableauFile
# This is probably not that Pythonic but whatevs
class TableauDocument(TableauBase):
    def __init__(self):
        TableauBase.__init__(self)
        self._datasources = []
        self._document_type = None
        self.parameters = None

    @property
    def datasources(self) -> List[TableauDatasource]:
        return self._datasources

    @property
    def document_type(self) -> str:
        return self._document_type

    # Basically an abstract base class, but not sure if there is value in implementing as such
    def save_file(self, filename_no_extension: str, save_to_directory: Optional[str] = None) -> bool:
        return True


