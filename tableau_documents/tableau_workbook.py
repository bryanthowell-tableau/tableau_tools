# -*- coding: utf-8 -*-

from ..tableau_base import *
from tableau_datasource import TableauDatasource


class TableauWorkbook(TableauBase):
    def __init__(self, wb_string, logger_obj=None):
        self.logger = logger_obj
        self.log(u'Initializing a TableauWorkbook object')
        self.wb_string = wb_string
        if self.wb_string.find('.twb') != -1:
            self.log(u".twb found in wb_string, assuming it is actually a filename. Opening file")
            fh = open(self.wb_string, 'rb')
            self.wb_string = fh.read()
        self.wb = StringIO(self.wb_string)
        self.start_xml = ""
        self.end_xml = ""
        self.datasources = {}
        start_flag = True
        ds_flag = False
        current_ds = ""

        if self.logger is not None:
            self.enable_logging(self.logger)

        for line in self.wb:
            # Start parsing the datasources
            if start_flag is True and ds_flag is False:
                self.start_xml += line
            if start_flag is False and ds_flag is False:
                self.end_xml += line
            if ds_flag is True:
                current_ds += line
                # Break and load the datasource
                if line.find(u"</datasource>") != -1:
                    self.log(u"Building TableauDatasource object")
                    ds_obj = TableauDatasource(current_ds, logger_obj=self.logger)
                    self.datasources[ds_obj.get_datasource_name()] = ds_obj
                    current_ds = ""
            if line.find(u"<datasources") != -1 and start_flag is True:
                start_flag = False
                ds_flag = True

            if line.find(u"</datasources>") != -1 and ds_flag is True:
                self.end_xml += line
                ds_flag = False

    def get_datasources(self):
        self.start_log_block()
        ds = self.datasources
        self.end_log_block()
        return ds

    def get_workbook_xml(self):
        self.start_log_block()
        xml = self.start_xml
        for ds in self.datasources:
            self.log(u'Adding in XML from datasource {}'.format(ds))
            xml += self.datasources.get(ds).get_datasource_xml()
        xml += self.end_xml
        self.end_log_block()
        return xml

    def save_workbook_xml(self, filename):
        self.start_log_block()
        try:
            lh = open(filename, 'wb')
            lh.write(self.get_workbook_xml())
            lh.close()
            self.end_log_block()
        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename))
            self.end_log_block()
            raise