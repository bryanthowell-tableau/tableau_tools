# -*- coding: utf-8 -*-

from ..tableau_base import *
from tableau_datasource import TableauDatasource
from tableau_parameters import TableauParameters
from tableau_document import TableauDocument
import os
import codecs


class TableauWorkbook(TableauDocument):
    def __init__(self, twb_filename, logger_obj=None):
        TableauDocument.__init__(self)
        self._document_type = u'workbook'
        self.parameters = None
        self.logger = logger_obj
        self.log(u'Initializing a TableauWorkbook object')
        self.twb_filename = twb_filename
        # Check the filename
        if self.twb_filename.find('.twb') == -1:
            raise InvalidOptionException(u'Must input a .twb filename that exists')
        self.build_document_objects(self.twb_filename)


#        if self.logger is not None:
#            self.enable_logging(self.logger)

    def build_document_objects(self, filename):
        wb_fh = codecs.open(filename, 'r', encoding='utf-8')
        ds_fh = codecs.open(u'temp_ds.txt', 'w', encoding='utf-8')

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
        os.remove(u'temp_ds.txt')

        self.log(u"Building TableauDatasource objects")
        datasource_elements = ds_xml.getroot().findall(u'datasource')
        if datasource_elements is None:
            raise InvalidOptionException(u'Error with the datasources from the workbook')
        for datasource in datasource_elements:
            if datasource.get(u'name') == u'Parameters':
                self.log(u'Found embedded Parameters datasource, creating TableauParameters object')
                self.parameters = TableauParameters(datasource, self.logger)
            else:
                ds = TableauDatasource(datasource, self.logger)
                self._datasources.append(ds)

    def add_parameters_to_workbook(self):
        """
        :rtype: TableauParameters
        """
        if self.parameters is not None:
            return self.parameters
        else:
            self.parameters = TableauParameters(logger_obj=self.logger)
            return self.parameters

    def save_file(self, filename_no_extension, save_to_directory=None):
        """
        :param filename_no_extension: Filename to save the XML to. Will append .twb if not found
        :type filename_no_extension: unicode
        :type save_to_directory: unicode
        :rtype: bool
        """
        self.start_log_block()
        try:
            orig_wb = codecs.open(self.twb_filename, 'r', encoding='utf-8')
            if filename_no_extension.find('.twb') == -1:
                filename_no_extension += '.twb'
            self.log(u'Saving to {}'.format(filename_no_extension))
            lh = codecs.open(filename_no_extension, 'w', encoding='utf-8')
            # Stream through the file, only pulling the datasources section
            ds_flag = None

            for line in orig_wb:
                # Skip the lines of the original datasource and sub in the new one
                if line.find("<datasources") != -1 and ds_flag is None:
                    self.log(u'Found the first of the datasources section')
                    ds_flag = True

                if ds_flag is not True:
                    lh.write(line)

                # Add in the modified datasources
                if line.find("</datasources>") != -1 and ds_flag is True:
                    self.log(u'Adding in the newly modified datasources')
                    ds_flag = False
                    lh.write('<datasources>\n')

                    final_datasources = []
                    if self.parameters is not None:
                        self.log(u'Adding parameters datasource back in')
                        final_datasources.append(self.parameters)
                        final_datasources.extend(self.datasources)
                    else:
                        final_datasources = self.datasources
                    for ds in final_datasources:
                        self.log(u'Writing datasource XML into the workbook')
                        ds_string = ds.get_datasource_xml()
                        if isinstance(ds_string, bytes):
                            final_string = ds_string.decode(u'utf-8')
                        else:
                            final_string = ds_string
                        lh.write(final_string)
                    lh.write('</datasources>\n')
            lh.close()
            self.end_log_block()
            return True

        except IOError:
            self.log(u"Error: File '{} cannot be opened to write to".format(filename_no_extension))
            self.end_log_block()
            raise