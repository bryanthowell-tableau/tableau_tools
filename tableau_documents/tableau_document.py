# -*- coding: utf-8 -*-
from typing import Union, Any, Optional, List, Dict, Tuple
from abc import ABC, abstractmethod

class TableauDocument(ABC):

    @abstractmethod
    def get_xml_string(self) -> str:
        pass

