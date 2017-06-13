# -*- coding: utf-8 -*-

from ..tableau_base import *
from tableau_datasource import TableauDatasource


class TableauWorkbook(TableauBase):
    def __init__(self, wb_string, logger_obj=None):
        TableauBase.__init__(self)
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


class TableauWorkbook3(TableauBase):
    def __init__(self, twb_filename, logger_obj=None):
        TableauBase.__init__(self)
        self.logger = logger_obj
        self.log(u'Initializing a TableauWorkbook object')
        self.twb_filename = twb_filename
        self.datasources = []
        # Check the filename
        if self.twb_filename.find('.twb') == -1:
            raise InvalidOptionException(u'Must input a .twb filename that exists')
        self.build_document_objects(self.twb_filename)

        if self.logger is not None:
            self.enable_logging(self.logger)

    def build_document_objects(self, filename):
        wb_fh = open(filename, 'rb')
        ds_fh = open(u'temp_ds.txt', 'wb')

        # Stream through the file, only pulling the datasources section
        ds_flag = None
        # Skip all the metadata
        metadata_flag = None
        for line in wb_fh:
            # Grab the datasources

            if line.find(u"<metadata-records") != -1 and metadata_flag is None:
                metadata_flag = True
            if ds_flag is True and metadata_flag is not True:
                ds_fh.write(line)
            if line.find(u"<datasources") != -1 and ds_flag is None:
                ds_flag = True
                ds_fh.write("<datasources xmlns:user='http://www.tableausoftware.com/xml/user'>\n")
            if line.find(u"</metadata-records") != -1 and metadata_flag is True:
                metadata_flag = False

            if line.find(u"</datasources>") != -1 and ds_flag is True:
                ds_fh.close()
                break
        wb_fh.close()

        utf8_parser = etree.XMLParser(encoding='utf-8')
        ds_xml = etree.parse(u'temp_ds.txt', parser=utf8_parser)

        self.log(u"Building TableauDatasource objects")
        datasource_elements = ds_xml.getroot().findall(u'datasource')
        if datasource_elements is None:
            raise InvalidOptionException(u'Error with the datasources from the workbook')
        for datasource in datasource_elements:
            print datasource
            ds = TableauDatasource(datasource)
            self.datasources.append(ds)

    def get_datasources(self):
        """
        :rtype: list[TableauDatasource]
        """
        self.start_log_block()
        ds = self.datasources
        self.end_log_block()
        return ds

    def save_workbook_xml(self, filename):
        """
        :param filename: Filename to save the XML to. Will append .twb if not found
        :type filename: unicode
        :return:
        """
        self.start_log_block()
        try:
            orig_wb = open(self.twb_filename, 'rb')
            if filename.find('.twb') == -1:
                filename += '.twb'
            lh = open(filename, 'wb')
            # Stream through the file, only pulling the datasources section
            ds_flag = None

            for line in orig_wb:
                # Skip the lines of the original datasource and sub in the new one
                if line.find("<datasources") != -1 and ds_flag is None:
                    ds_flag = True

                if ds_flag is False:
                    lh.write(line)

                # Add in the modified datasources
                if line.find("</datasources>") != -1 and ds_flag is True:
                    ds_flag = False
                    lh.write('<datasources>\n')
                    for ds in self.datasources:
                        lh.write(ds.get_datasource_xml())
                    lh.write('</datasources>\n')
            lh.close()
            self.end_log_block()
        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename))
            self.end_log_block()
            raise