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


class TableauColumns(TableauBase):
    def __init__(self, columns_list, logger_obj=None):
        self.logger = logger_obj
        self.log_debug('Initializing a TableauColumns object')
        self.__translation_dict = None
        # List of lxml columns objects
        self.columns_list = columns_list

    def set_translation_dict(self, trans_dict):
        self.start_log_block()
        self.__translation_dict = trans_dict
        self.end_log_block()

    def translate_captions(self):
        self.start_log_block()
        for column in self.columns_list:
            if column.getroot().get('caption') is None:
                trans = self.__find_translation(column.getroot().get('name'))
            else:
                # Try to match caption first, if not move to name
                trans = self.__find_translation(column.getroot().get('caption'))
                if trans is None:
                    trans = self.__find_translation(column.getroot().get('name'))
            if trans is not None:
                column.getroot().set('caption', trans)
        self.end_log_block()

    def __find_translation(self, match_str):
        self.start_log_block()
        d = self.__translation_dict.get(match_str)
        self.end_log_block()
        return d

    def get_column_by_name(self, column_name):
        """
        :type parameter_name: unicode
        :rtype: TableauParameter
        """
        for c in self.columns_list:
            if c.name == column_name:
                return c
        else:
            raise NoMatchFoundException('No column named {}'.format(column_name))


class TableauColumn(TableauBase):
    def __init__(self, column_xml_obj, logger_obj=None):
        """
        :type column_xml_obj: etree.Element
        :param logger_obj:
        """
        self.logger = logger_obj
        self.log_debug('Initializing TableauColumn object')
        self.xml_obj = column_xml_obj

    @property
    def alias(self):
        return self.xml_obj.get('caption')

    @alias.setter
    def alias(self, alias):
        """
        :type alias: unicode
        :return:
        """
        self.xml_obj.set('caption', alias)

    @property
    def datatype(self):
        return self.xml_obj.get('datatype')

    @alias.setter
    def datatype(self, datatype):
        """
        :type datatype: unicode
        :return:
        """
        if datatype.lower() not in ['string', 'integer', 'datetime', 'date', 'real', 'boolean']:
            raise InvalidOptionException("{} is not a valid datatype".format(datatype))
        self.xml_obj.set('datatype', datatype)

    @property
    def column_name(self):
        return self.xml_obj.get('name')

    @column_name.setter
    def column_name(self, column_name):
        """
        :type column_name: unicode
        :return:
        """
        if column_name[0] == "[" and column_name[-1] == "]":
            new_column_name = column_name
        else:
            new_column_name = "[{}]".format(column_name)
        self.xml_obj.set('name', new_column_name)

    @property
    def dimension_or_measure(self):
        return self.xml_obj.get('role')

    @dimension_or_measure.setter
    def dimension_or_measure(self, dimension_or_measure):
        """
        :type dimension_or_measure: unicode
        :return:
        """
        final_dimension_or_measure = dimension_or_measure.lower()
        if final_dimension_or_measure not in ['dimension', 'measure']:
            raise InvalidOptionException('dimension_or_measure must be "dimension" or "measure"')
        self.xml_obj.set('role', final_dimension_or_measure)

    @property
    def aggregation_type(self):
        return self.xml_obj.get('type')

    @aggregation_type.setter
    def aggregation_type(self, aggregation_type):
        """
        :type aggregation_type: unicode
        :return:
        """
        final_aggregation_type = aggregation_type.lower()
        if final_aggregation_type not in ['ordinal', 'nominal', 'quantitative']:
            raise InvalidOptionException('aggregation_type must be "ordinal", "nominal" or "quantiative"')
        self.xml_obj.set('type', final_aggregation_type)


class TableauHierarchies(TableauBase):
    def __init__(self, hierarchies_xml, logger_obj=None):
        self.logger = logger_obj
        self.log_debug('Initializing TableauHierarchies object')
        self.xml_obj = hierarchies_xml
        self.hierarchies = self.xml_obj.findall('./drill-path')

    def get_hierarchy_by_name(self, hierarchy_name):
        """
        :type hierarchy_name: unicode
        :rtype:
        """
        for h in self.hierarchies:
            if h.get('name') == hierarchy_name:
                return h
        else:
            raise NoMatchFoundException('No hierarchy named {}'.format(hierarchy_name))


class TableauHierarchy(TableauBase):
    def __init__(self, hierarchy_xml, logger_obj=None):
        self.logger = logger_obj
        self.log_debug('Initializing TableauHierarchies object')
        self.xml_obj = hierarchy_xml
        self._name = self.xml_obj.get('name')
        self._fields = []
        for f in self.xml_obj:
            self._fields.append(f.text)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, new_name):
        """
        :type new_name: unicode
        :return:
        """
        self.xml_obj.set('name', new_name)

    @property
    def fields(self):
        return self._fields

    def set_existing_field(self, field_position, field_value):
        """
        :type field_position: int
        :type field_value: unicode
        :return:
        """
        if field_position < 0:
            raise InvalidOptionException('Field position must be positive integer')
        if field_position >= len(self._fields):
            raise InvalidOptionException('Only {} fields, field_position {} too high'.format(len(self._fields),
                                                                                              field_position))
        if field_value[0] == "[" and field_value[-1] == "]":
            self._fields[field_position] = field_value
        else:
            self._fields[field_position] = "[{}]".format(field_value)

    def add_field(self, field_value):
        """
        :type field_value: unicode
        :return:
        """
        if field_value[0] == "[" and field_value[-1] == "]":
            self._fields.append(field_value)
        else:
            self._fields.append("[{}]".format(field_value))

    def remove_field(self, field_position):
        """
        :type field_position: int
        :return:
        """
        if field_position < 0:
            raise InvalidOptionException('Field position must be positive integer')
        if field_position >= len(self._fields):
            raise InvalidOptionException('Only {} fields, field_position {} too high'.format(len(self._fields),
                                                                                              field_position))
        del self._fields[field_position]
