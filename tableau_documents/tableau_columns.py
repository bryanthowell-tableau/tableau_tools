# -*- coding: utf-8 -*-
from typing import Union, Any, Optional, List, Dict, Tuple
import xml.etree.ElementTree as ET

# from tableau_tools.tableau_base import TableauBase
from tableau_tools.tableau_exceptions import *
from tableau_tools.logger import Logger
from tableau_tools.logging_methods import LoggingMethods

from .tableau_parameters import TableauParameter


class TableauColumns(LoggingMethods):
    def __init__(self, columns_list: List[ET.Element], logger_obj: Optional[Logger] = None):
        self.logger = logger_obj
        self.log_debug('Initializing a TableauColumns object')
        self.columns_list: List[ET.Element] = columns_list

    def translate_captions(self, translation_dict: Dict):
        self.start_log_block()
        for column in self.columns_list:
            if column.get('caption') is None:
                trans = translation_dict.get(column.get('name'))
            else:
                # Try to match caption first, if not move to name
                trans = translation_dict.get(column.get('caption'))
                if trans is None:
                    trans = translation_dict.get(column.get('name'))
            if trans is not None:
                column.set('caption', trans)
        self.end_log_block()

    def get_column_by_name(self, column_name: str) -> TableauParameter:
        for c in self.columns_list:
            if c.get('name') == '[{}]]'.format(column_name) or c.get('caption') == column_name:
                return c
        else:
            raise NoMatchFoundException('No column named {}'.format(column_name))


class TableauColumn(LoggingMethods):
    def __init__(self, column_xml_obj: ET.Element, logger_obj: Optional[Logger] = None):
        self.logger = logger_obj
        self.log_debug('Initializing TableauColumn object')
        self.xml_obj = column_xml_obj

    @property
    def alias(self) -> str:
        return self.xml_obj.get('caption')

    @alias.setter
    def alias(self, alias: str):
        self.xml_obj.set('caption', alias)

    @property
    def datatype(self) -> str:
        return self.xml_obj.get('datatype')

    @datatype.setter
    def datatype(self, datatype: str):
        if datatype.lower() not in ['string', 'integer', 'datetime', 'date', 'real', 'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))
        self.xml_obj.set('datatype', datatype)

    @property
    def column_name(self) -> str:
        return self.xml_obj.get('name')

    @column_name.setter
    def column_name(self, column_name: str):
        if column_name[0] == "[" and column_name[-1] == "]":
            new_column_name = column_name
        else:
            new_column_name = "[{}]".format(column_name)
        self.xml_obj.set('name', new_column_name)

    @property
    def dimension_or_measure(self) -> str:
        return self.xml_obj.get('role')

    @dimension_or_measure.setter
    def dimension_or_measure(self, dimension_or_measure: str):
        final_dimension_or_measure = dimension_or_measure.lower()
        if final_dimension_or_measure not in ['dimension', 'measure']:
            raise InvalidOptionException('dimension_or_measure must be "dimension" or "measure"')
        self.xml_obj.set('role', final_dimension_or_measure)

    @property
    def aggregation_type(self) -> str:
        return self.xml_obj.get('type')

    @aggregation_type.setter
    def aggregation_type(self, aggregation_type: str):
        final_aggregation_type = aggregation_type.lower()
        if final_aggregation_type not in ['ordinal', 'nominal', 'quantitative']:
            raise InvalidOptionException('aggregation_type must be "ordinal", "nominal" or "quantiative"')
        self.xml_obj.set('type', final_aggregation_type)


class TableauHierarchies(LoggingMethods):
    def __init__(self, hierarchies_xml: ET.Element, logger_obj: Optional[Logger] = None):
        self.logger = logger_obj
        self.log_debug('Initializing TableauHierarchies object')
        self.xml_obj = hierarchies_xml
        self.hierarchies = self.xml_obj.findall('./drill-path')

    def get_hierarchy_by_name(self, hierarchy_name: str):
        for h in self.hierarchies:
            if h.get('name') == hierarchy_name:
                return h
        else:
            raise NoMatchFoundException('No hierarchy named {}'.format(hierarchy_name))


class TableauHierarchy(LoggingMethods):
    def __init__(self, hierarchy_xml: ET.Element, logger_obj: Optional[Logger] = None):
        self.logger = logger_obj
        self.log_debug('Initializing TableauHierarchies object')
        self.xml_obj: ET.Element = hierarchy_xml
        self._name: str = self.xml_obj.get('name')
        self._fields: List[str] = []
        for f in self.xml_obj:
            self._fields.append(f.text)

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, new_name: str):
        self.xml_obj.set('name', new_name)

    @property
    def fields(self) -> List[str]:
        return self._fields

    def set_existing_field(self, field_position: int, field_value: str):
        if field_position < 0:
            raise InvalidOptionException('Field position must be positive integer')
        if field_position >= len(self._fields):
            raise InvalidOptionException('Only {} fields, field_position {} too high'.format(len(self._fields),
                                                                                              field_position))
        if field_value[0] == "[" and field_value[-1] == "]":
            self._fields[field_position] = field_value
        else:
            self._fields[field_position] = "[{}]".format(field_value)

    def add_field(self, field_value: str):
        if field_value[0] == "[" and field_value[-1] == "]":
            self._fields.append(field_value)
        else:
            self._fields.append("[{}]".format(field_value))

    def remove_field(self, field_position: int):
        if field_position < 0:
            raise InvalidOptionException('Field position must be positive integer')
        if field_position >= len(self._fields):
            raise InvalidOptionException('Only {} fields, field_position {} too high'.format(len(self._fields),
                                                                                              field_position))
        del self._fields[field_position]
