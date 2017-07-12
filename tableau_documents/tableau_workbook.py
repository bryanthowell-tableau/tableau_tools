# -*- coding: utf-8 -*-

from ..tableau_base import *
from tableau_datasource import TableauDatasource
from tableau_document import TableauDocument
import os


class TableauWorkbook(TableauDocument):
    def __init__(self, twb_filename, logger_obj=None):
        TableauDocument.__init__(self)
        self._document_type = u'workbook'
        self.logger = logger_obj
        self.log(u'Initializing a TableauWorkbook object')
        self.twb_filename = twb_filename
        self.temp_filename = None
        # Check the filename
        if self.twb_filename.find('.twb') == -1:
            raise InvalidOptionException(u'Must input a .twb filename that exists')
        self.build_document_objects(self.twb_filename)

        if self.logger is not None:
            self.enable_logging(self.logger)

    def __del__(self):
        if self.temp_filename is not None:
            os.remove(self.temp_filename)

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
        self.temp_filename = u'temp_ds.txt'

        self.log(u"Building TableauDatasource objects")
        datasource_elements = ds_xml.getroot().findall(u'datasource')
        if datasource_elements is None:
            raise InvalidOptionException(u'Error with the datasources from the workbook')
        for datasource in datasource_elements:
            ds = TableauDatasource(datasource, self.logger)
            self._datasources.append(ds)

    def save_file(self, filename_no_extension, save_to_directory=None):
        """
        :param filename_no_extension: Filename to save the XML to. Will append .twb if not found
        :type filename_no_extension: unicode
        :type save_to_directory: unicode
        :rtype: bool
        """
        self.start_log_block()
        try:
            orig_wb = open(self.twb_filename, 'rb')
            if filename_no_extension.find('.twb') == -1:
                filename_no_extension += '.twb'
            lh = open(filename_no_extension, 'wb')
            # Stream through the file, only pulling the datasources section
            ds_flag = None

            for line in orig_wb:
                # Skip the lines of the original datasource and sub in the new one
                if line.find("<datasources") != -1 and ds_flag is None:
                    ds_flag = True

                if ds_flag is not True:
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
            return True

        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename_no_extension))
            self.end_log_block()
            raise