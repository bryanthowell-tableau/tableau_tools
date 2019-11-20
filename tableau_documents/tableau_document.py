# -*- coding: utf-8 -*-
from typing import Union, Any, Optional, List, Dict, Tuple
from abc import ABC, abstractmethod

from tableau_tools.logging_methods import LoggingMethods
from ..tableau_exceptions import *
from ..logger import Logger
from .tableau_datasource import TableauDatasource

# This class is just a shell so that TableauWorkbook and TableauDatasource can look the same from TableauFile
# This is probably not that Pythonic but whatevs
class TableauDocument(LoggingMethods, ABC):
    def __init__(self):
        self._datasources = []
        self._document_type = None
        self.parameters = None


    @property
    @abstractmethod
    def datasources(self) -> List[TableauDatasource]:
        #return self._datasources
        pass

    @property
    @abstractmethod
    def document_type(self) -> str:
        pass
        #return self._document_type

    # Basically an abstract base class, but not sure if there is value in implementing as such
    @abstractmethod
    def save_file(self, filename_no_extension: str, save_to_directory: Optional[str] = None) -> bool:
        pass


