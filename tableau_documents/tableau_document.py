# -*- coding: utf-8 -*-

from ..tableau_base import TableauBase


# This class is just a shell so that TableauWorkbook and TableauDatasource can look the same from TableauFile
# This is probably not that Pythonic but whatevs
class TableauDocument(TableauBase):
    def __init__(self):
        TableauBase.__init__(self)
        self._datasources = []
        self._document_type = None

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
        self.log(u'Initializing a TableauColumns object')
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


class TableauParameterColumns(TableauBase):
    def __init__(self, columns_list, logger_obj=None):
        self.logger = logger_obj
        self.log(u'Initializing a TableauColumns object')
        # List of lxml columns objects
        self.columns_list = columns_list

    #def get_parameter_by_name(self):
        #for col in self.columns_list:
           # for col.get()

