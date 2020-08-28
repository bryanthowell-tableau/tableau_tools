# -*- coding: utf-8 -*-
import os
import codecs
import xml.etree.ElementTree as ET
from typing import Union, Any, Optional, List, Dict, Tuple
import io

from tableau_tools.logging_methods import LoggingMethods
from tableau_tools.logger import Logger
from tableau_tools.tableau_exceptions import *

# TableauDocument defines ABC - interface
from .tableau_document import TableauDocument
from .tableau_datasource import TableauDatasource
from .tableau_parameters import TableauParameters


# Historically, this was just a file wrapper. That functionality has moved to the TWB class
# This is now a stub for any eventual XML modification within the workbook
class TableauWorkbook(LoggingMethods, TableauDocument):
    def __init__(self, twb_filename: str, logger_obj: Optional[Logger] = None):
        #TableauDocument.__init__(self)
        self.document_type = 'workbook'
        self.parameters = None
        self.logger = logger_obj
        self.log('Initializing a TableauWorkbook object')
        self.twb_filename = twb_filename
        # Check the filename
        if self.twb_filename.find('.twb') == -1:
            raise InvalidOptionException('Must input a .twb filename that exists')
        self.datasources: List[TableauDatasource] = []

    def build_datasource_objects(self, datasource_xml: ET.Element):
        datasource_elements = datasource_xml.getroot().findall('datasource')
        if datasource_elements is None:
            raise InvalidOptionException('Error with the datasources from the workbook')
        for datasource in datasource_elements:
            if datasource.get('name') == 'Parameters':
                self.log('Found embedded Parameters datasource, creating TableauParameters object')
                self.parameters = TableauParameters(datasource, self.logger)
            else:
                ds = TableauDatasource(datasource, self.logger)
                self.datasources.append(ds)

    def add_parameters_to_workbook(self) -> TableauParameters:
        if self.parameters is not None:
            return self.parameters
        else:
            self.parameters = TableauParameters(logger_obj=self.logger)
            return self.parameters

    def get_xml_string(self) -> str:
        self.start_log_block()
        try:
            orig_wb = codecs.open(self.twb_filename, 'r', encoding='utf-8')

            # Handle all in-memory as a file-like object
            lh = io.StringIO()
            # Stream through the file, only pulling the datasources section
            ds_flag = None

            for line in orig_wb:
                # Skip the lines of the original datasource and sub in the new one
                if line.find("<datasources") != -1 and ds_flag is None:
                    self.log('Found the first of the datasources section')
                    ds_flag = True

                if ds_flag is not True:
                    lh.write(line)

                # Add in the modified datasources
                if line.find("</datasources>") != -1 and ds_flag is True:
                    self.log('Adding in the newly modified datasources')
                    ds_flag = False
                    lh.write('<datasources>\n')

                    final_datasources = []
                    if self.parameters is not None:
                        self.log('Adding parameters datasource back in')
                        final_datasources.append(self.parameters)
                        final_datasources.extend(self.datasources)
                    else:
                        final_datasources = self.datasources
                    for ds in final_datasources:
                        self.log('Writing datasource XML into the workbook')
                        ds_string = ds.get_xml_string()
                        print(ds_string)
                        if isinstance(ds_string, bytes):
                            final_string = ds_string.decode('utf-8')
                        else:
                            final_string = ds_string
                        lh.write(final_string)
                    lh.write('</datasources>\n')
            # Reset back to the beginning
            lh.seek(0)
            final_xml_string = lh.getvalue()
            lh.close()
            self.end_log_block()
            return final_xml_string

        except IOError:
            self.log("Error: File '{} cannot be opened to read from".format(self.twb_filename))
            self.end_log_block()
            raise