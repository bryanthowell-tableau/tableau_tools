# -*- coding: utf-8 -*-

from ..tableau_base import *


class TableauColumns(TableauBase):
    def __init__(self, column_lines, logger_obj=None):
        self.logger = logger_obj
        self.log(u'Initializing a TableauColumns object')
        self.__translation_dict = None
        # Building from a <column> tag
        self.xml_obj = None
        self.columns_text = "<columns xmlns:user='http://www.tableausoftware.com/xml/user'>\n" + column_lines + "</columns>"
        self.columns_text = self.columns_text.strip()
        self.log(u'Looking at columns:\n {}'.format(self.columns_text))
        utf8_parser = etree.XMLParser(encoding='utf-8')
        xml = etree.parse(StringIO(self.columns_text), parser=utf8_parser)
        self.columns_obj = xml.getroot()
        # xml = etree.fromstring(connection_line)

    def set_translation_dict(self, trans_dict):
        self.start_log_block()
        self.__translation_dict = trans_dict
        self.end_log_block()

    def translate_captions(self):
        self.start_log_block()
        for column in self.columns_obj:
            if column.get('caption') is None:
                trans = self.__find_translation(column.get('name'))
            else:
                # Try to match caption first, if not move to name
                trans = self.__find_translation(column.get('caption'))
                if trans is None:
                    trans = self.__find_translation(column.get('name'))
            if trans is not None:
                column.set('caption', trans)
        self.end_log_block()

    def __find_translation(self, match_str):
        self.start_log_block()
        d = self.__translation_dict.get(match_str)
        self.end_log_block()
        return d

    def get_xml_string(self):
        self.start_log_block()
        xml_with_extra_tags = etree.tostring(self.columns_obj, encoding='utf8')
        # Slice off the extra connection ending tag
        first_tag_place = len('<columns xmlns:user="http://www.tableausoftware.com/xml/user">') + 1
        xml = xml_with_extra_tags[first_tag_place:xml_with_extra_tags.find('</columns>')-1]
        self.end_log_block()
        return xml