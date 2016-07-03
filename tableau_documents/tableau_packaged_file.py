# -*- coding: utf-8 -*-

import zipfile
import os
import shutil
from ..tableau_base import *
from tableau_datasource import TableauDatasource
from tableau_workbook import TableauWorkbook


# Represents a TWBX or TDSX and allows manipulation of the XML objects inside via their related object
class TableauPackagedFile(TableauBase):
    def __init__(self, zip_file_obj, logger_obj=None):
        self.logger = logger_obj
        self.log(u'TableauPackagedFile initializing')
        self.zf = zipfile.ZipFile(zip_file_obj)
        self.xml_name = None
        self.type = None  # either 'twbx' or 'tdsx'
        self.tableau_object = None
        self.other_files = []
        for name in self.zf.namelist():
            # Ignore anything in the subdirectories
            if name.find('/') == -1:
                if name.endswith('.tds'):
                    self.log(u'Detected a .TDS file in archive, creating a TableauDatasource object')
                    self.type = 'tdsx'
                    self.xml_name = name
                    tds_file_obj = self.zf.open(self.xml_name)
                    self.tableau_object = TableauDatasource(tds_file_obj.read(), self.logger)
                elif name.endswith('.twb'):
                    self.log(u'Detected a .TWB file in archive, creating a TableauDatasource object')
                    self.type = 'twbx'
                    self.xml_name = name
                    twb_file_obj = self.zf.open(self.xml_name)
                    self.tableau_object = TableauWorkbook(twb_file_obj.read(), self.logger)

            else:
                self.other_files.append(name)

    def get_type(self):
        self.start_log_block()
        t = self.type
        self.end_log_block()
        return t

    def get_tableau_object(self):
        self.start_log_block()
        obj = self.tableau_object
        self.end_log_block()
        return obj

    # Appropriate extension added if needed
    def save_new_packaged_file(self, new_filename_no_extension):
        self.start_log_block()
        new_filename = new_filename_no_extension.split('.') # simple algorithm to kill extension

        # Save the object down
        if self.type == 'twbx':
            save_filename = new_filename[0] + '.twbx'
            new_zf = zipfile.ZipFile(save_filename, 'w')
            self.log(u'Creating temporary XML file {}'.format(self.xml_name))
            self.tableau_object.save_workbook_xml(self.xml_name)
            new_zf.write(self.xml_name)
            os.remove(self.xml_name)
        elif self.type == 'tdsx':
            save_filename = new_filename[0] + '.tdsx'
            new_zf = zipfile.ZipFile(save_filename, 'w')
            self.log(u'Creating temporary XML file {}'.format(self.xml_name))
            self.tableau_object.save_datasource_xml(self.xml_name)
            new_zf.write(self.xml_name)
            os.remove(self.xml_name)
            self.log(u'Removed file {}'.format(save_filename))

        temp_directories_to_remove = {}
        for filename in self.other_files:
            self.log(u'Extracting file {} temporarily'.format(filename))
            self.zf.extract(filename)
            new_zf.write(filename)
            os.remove(filename)
            self.log(u'Removed file {}'.format(filename))
            lowest_level = filename.split('/')
            temp_directories_to_remove[lowest_level[0]] = True

        # Cleanup all the temporary directories
        for directory in temp_directories_to_remove:
            shutil.rmtree(directory)
        new_zf.close()
        self.zf.close()

        # Return the filename so it can be opened from disk by other objects
        self.end_log_block()
        return save_filename